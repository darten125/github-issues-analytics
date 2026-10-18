"""Small, testable boundary between Airflow tasks and the existing STG loader."""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from datetime import timezone
from typing import Any

from .config import REPOSITORIES
from .github_client import GitHubClient
from .loader import load_repository, parse_github_time
from .store import StgStore

LOGGER = logging.getLogger(__name__)
MODES = ("backfill", "sync", "audit")


def normalize_run_params(params: Mapping[str, Any]) -> dict:
    """Validate the actual DagRun params, especially the full-load interlock."""
    repository = params.get("repository")
    mode = params.get("mode")
    created_since = params.get("created_since")
    max_issues = params.get("max_issues")
    full_load = params.get("full_load")

    if not isinstance(repository, str) or repository not in (*REPOSITORIES, "all"):
        raise ValueError("repository must be one of the three configured repositories or all")
    if not isinstance(mode, str) or mode not in MODES:
        raise ValueError("mode must be backfill, sync, or audit")
    if not isinstance(created_since, str):
        raise ValueError("created_since must be a timestamp with time zone")
    try:
        parse_github_time(created_since)
    except (TypeError, ValueError) as exc:
        raise ValueError("created_since must be a valid timestamp with time zone") from exc
    if type(full_load) is not bool:
        raise ValueError("full_load must be a boolean")
    if max_issues is not None and (type(max_issues) is not int or max_issues < 1):
        raise ValueError("max_issues must be a positive integer or null")
    if full_load and max_issues is not None:
        raise ValueError("full_load=true requires max_issues=null")
    if not full_load and max_issues is None:
        raise ValueError("max_issues=null requires explicit full_load=true")

    repositories = list(REPOSITORIES) if repository == "all" else [repository]
    return {
        "repository": repository,
        "repositories": repositories,
        "mode": mode,
        "created_since": created_since,
        "max_issues": max_issues,
        "full_load": full_load,
    }


def check_resources() -> bool:
    """Check credentials are present and PostgreSQL is reachable, without GitHub traffic."""
    if not os.getenv("GITHUB_TOKEN", "").strip():
        raise ValueError("GITHUB_TOKEN is missing in the Airflow task environment")
    store = StgStore.from_env()
    try:
        with store.connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            if cursor.fetchone()[0] != 1:
                raise RuntimeError("PostgreSQL availability check failed")
    finally:
        store.close()
    return True


def prepare_schema() -> bool:
    """Apply existing idempotent STG migrations before any API request."""
    store = StgStore.from_env()
    try:
        store.apply_schema()
    finally:
        store.close()
    return True


def load_selected_repository(settings: Mapping[str, Any], full_name: str) -> dict | None:
    """Run one selected repository in this task process; do not swallow errors."""
    if full_name not in settings["repositories"]:
        return None
    token = os.getenv("GITHUB_TOKEN", "").strip()
    if not token:
        raise ValueError("GITHUB_TOKEN is missing in the Airflow task environment")
    store = None
    client = None
    try:
        store = StgStore.from_env()
        client = GitHubClient(token)
        LOGGER.info("Starting STG %s for %s; max_issues=%s",
                    settings["mode"], full_name, settings["max_issues"])
        batch_id = load_repository(
            client, store, full_name, parse_github_time(settings["created_since"]),
            settings["max_issues"], mode=settings["mode"],
        )
        LOGGER.info("STG loader finished for %s in batch %s; checking completeness next",
                    full_name, batch_id)
        return {"repository": full_name, "batch_id": str(batch_id)}
    finally:
        if client is not None:
            client.session.close()
        if store is not None:
            store.close()


def verify_batch(settings: Mapping[str, Any], outcome: dict | None) -> dict | None:
    """Read the persisted batch status; a successful HTTP loop alone is insufficient."""
    if outcome is None:
        return None
    repository = outcome["repository"]
    batch_id = outcome["batch_id"]
    if repository not in settings["repositories"]:
        raise RuntimeError("unexpected repository in STG load outcome")

    store = StgStore.from_env()
    try:
        with store.connection.cursor() as cursor:
            cursor.execute(
                "SELECT b.batch_id::text, b.repository_full_name, b.status, b.issue_scan_status, "
                "b.issue_scan_passes, b.issue_scan_issue_count, b.issues_selected, b.events_fetched, "
                "b.issue_versions_inserted, b.event_versions_inserted, "
                "(SELECT count(*) FROM stg.issue_event_loads l "
                " WHERE l.batch_id = b.batch_id AND l.status <> 'succeeded'), "
                "b.created_since, b.created_since_source "
                "FROM stg.load_batches b WHERE b.batch_id = %s::uuid",
                (batch_id,),
            )
            row = cursor.fetchone()
    finally:
        store.close()

    if row is None:
        raise RuntimeError(f"STG batch {batch_id} was not found")
    (found_id, found_repository, status, scan_status, passes, issue_count,
     issues_selected, events_fetched, issue_versions, event_versions, incomplete_attempts,
     created_since, created_since_source) = row
    if found_id != batch_id or found_repository != repository:
        raise RuntimeError(f"STG batch {batch_id} does not match the selected repository")
    if status != "succeeded":
        raise RuntimeError(f"STG batch {batch_id} has status {status}; load is not complete")
    expected_scan = "verified" if settings["full_load"] else "sample"
    if scan_status != expected_scan:
        raise RuntimeError(
            f"STG batch {batch_id} has issue_scan_status={scan_status}; expected {expected_scan}. "
            "The HTTP requests may have succeeded, but Issue enumeration is not confirmed."
        )
    if incomplete_attempts:
        raise RuntimeError(f"STG batch {batch_id} has {incomplete_attempts} unfinished/failed Issue histories")
    if (created_since_source != "recorded" or created_since is None
            or created_since != parse_github_time(settings["created_since"])):
        raise RuntimeError(f"STG batch {batch_id} did not record the requested created_since boundary")

    result = {
        "repository": repository,
        "batch_id": batch_id,
        "status": status,
        "issue_scan_status": scan_status,
        "issue_scan_passes": passes,
        "issue_scan_issue_count": issue_count,
        "issues_selected": issues_selected,
        "events_fetched": events_fetched,
        "issue_versions_inserted": issue_versions,
        "event_versions_inserted": event_versions,
        "created_since": created_since.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    LOGGER.info("Verified STG batch %s for %s: %s, %s, %s Issues, %s events",
                batch_id, repository, status, scan_status, issues_selected, events_fetched)
    return result


def build_summary(settings: Mapping[str, Any], *results: dict | None) -> dict:
    """Return only batch IDs, statuses and counters in XCom."""
    selected = [result for result in results if result is not None]
    if [result["repository"] for result in selected] != settings["repositories"]:
        raise RuntimeError("STG result list does not match selected repositories")
    summary = {"status": "succeeded", "mode": settings["mode"],
               "full_load": settings["full_load"], "batches": selected}
    LOGGER.info("STG DAG completed for %s with %s confirmed batches",
                ", ".join(settings["repositories"]), len(selected))
    return summary

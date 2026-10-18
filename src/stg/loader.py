"""Page-by-page orchestration of GitHub responses into STG."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from .github_client import GitHubApiError

LOGGER = logging.getLogger(__name__)
MAX_CONTROL_PASSES = 2


def parse_github_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("GitHub timestamp must include a time zone")
    return parsed.astimezone(timezone.utc)


def select_issues(page: list[dict], cutoff: datetime) -> tuple[list[dict], bool]:
    """Use the endpoint's descending creation order to stop at the cutoff."""
    selected = []
    for issue in page:
        if parse_github_time(issue["created_at"]) < cutoff:
            return selected, True
        if "pull_request" not in issue:
            selected.append(issue)
    return selected, False


def load_repository(client, store, full_name: str, cutoff: datetime,
                    max_issues: int | None = None, mode: str = "backfill"):
    if max_issues is not None and max_issues < 1:
        raise ValueError("max_issues must be positive")
    if mode not in ("backfill", "sync", "audit"):
        raise ValueError("invalid load mode")
    batch_id = store.start_batch(full_name, mode=mode)
    selected_count = 0
    seen_issue_ids = set()
    processed_events_ids = set()
    passes_started = 0
    last_ids = set()

    def process_issue(repository_id: int, issue: dict):
        issue_id = int(issue["id"])
        if issue_id in processed_events_ids:
            return
        if store.has_completed_events(issue_id, issue.get("updated_at"), mode):
            return
        store.start_issue_events(batch_id, repository_id, issue, mode=mode)
        try:
            for events_page in client.iter_issue_events(full_name, int(issue["number"])):
                store.save_events(batch_id, repository_id, issue_id, events_page)
            store.finish_issue_events(batch_id, issue_id, "succeeded")
            processed_events_ids.add(issue_id)
        except Exception as exc:
            message = str(exc) if isinstance(exc, GitHubApiError) else type(exc).__name__
            store.finish_issue_events(batch_id, issue_id, "failed", error_message=message)
            raise
        LOGGER.info("Stored issue %s#%s and its available events", full_name, issue["number"])

    def scan_issues(repository_id: int, *, control: bool) -> set[int]:
        nonlocal selected_count, passes_started, last_ids
        passes_started += 1
        current_ids = set()
        for page in client.iter_issues(full_name):
            selected, reached_cutoff = select_issues(page, cutoff)
            if max_issues is not None:
                selected = selected[:max_issues - selected_count]
            current_ids.update(int(issue["id"]) for issue in selected)
            if control:
                new_on_page = set()
                to_process = []
                for issue in selected:
                    issue_id = int(issue["id"])
                    if issue_id not in seen_issue_ids and issue_id not in new_on_page:
                        to_process.append(issue)
                        new_on_page.add(issue_id)
            else:
                to_process = selected
            store.save_issues(batch_id, repository_id, to_process, len(page))
            for issue in to_process:
                process_issue(repository_id, issue)
            seen_issue_ids.update(int(issue["id"]) for issue in selected)
            selected_count += len(selected)
            if reached_cutoff or (max_issues is not None and selected_count >= max_issues):
                break
        last_ids = current_ids
        return current_ids

    try:
        repository = client.get_repository(full_name)
        repository_id = int(repository["id"])
        store.save_repository(batch_id, repository)
        previous_ids = scan_issues(repository_id, control=False)
        if max_issues is not None:
            scan_status = "sample"
        else:
            scan_status = "unverified"
            for _ in range(MAX_CONTROL_PASSES):
                current_ids = scan_issues(repository_id, control=True)
                if current_ids == previous_ids:
                    scan_status = "verified"
                    break
                previous_ids = current_ids
            if scan_status == "unverified":
                LOGGER.warning("Issue ID sets did not stabilize for %s after %s passes", full_name, passes_started)
        store.record_issue_scan(batch_id, scan_status, passes_started, len(last_ids))
        store.finish_batch(batch_id, "succeeded")
        return batch_id
    except Exception as exc:
        message = str(exc) if isinstance(exc, GitHubApiError) else type(exc).__name__
        store.record_issue_scan(batch_id, "unverified", passes_started, len(last_ids))
        store.finish_batch(batch_id, "failed", error_message=message)
        raise

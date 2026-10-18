"""Normalize STG and atomically publish a selected set of repositories."""

from __future__ import annotations

import logging
from collections import defaultdict
from uuid import uuid4

from . import DdsError, RULES_VERSION
from .scd2 import build_history
from .source import _id, _time, read_source
from . import store
from .validation import validate_current

LOGGER = logging.getLogger(__name__)


def normalize(source):
    normalized: dict = {"repositories": [], "users": [], "labels": [], "issues": [],
                  "histories": {}, "links": {}, "warnings": []}
    users: dict = {}
    labels: dict = {}

    def provenance(raw, priority):
        return {"observed_at": raw["observed_at"], "source_priority": priority,
                "source_observation_id": raw["source_observation_id"],
                "source_kind": "issue" if priority else "event", "source_snapshot_id": raw["snapshot_id"]}

    def latest(target, key, value):
        previous = target.get(key)
        order = ("observed_at", "source_priority", "source_observation_id")
        if previous is None or tuple(value[k] for k in order) >= tuple(previous[k] for k in order):
            target[key] = value

    def user(data, raw, priority):
        if data is None:
            return None
        if not isinstance(data, dict):
            raise DdsError("invalid_user")
        user_id = _id(data.get("id"), "user.id")
        for field in ("login", "type"):
            if data.get(field) is not None and not isinstance(data[field], str):
                raise DdsError("invalid_user_attribute", details={"field": field})
        latest(users, user_id, {"user_id": user_id, "login": data.get("login"),
                               "user_type": data.get("type"), **provenance(raw, priority)})
        return user_id

    def label(data, repository_id, raw, priority, *, historical=False):
        if not isinstance(data, dict) or not isinstance(data.get("name"), str):
            raise DdsError("invalid_label", repository_id=repository_id)
        if historical and data.get("id") is None:
            return None
        label_id = _id(data.get("id"), "label.id")
        if label_id in labels and labels[label_id]["repository_id"] != repository_id:
            raise DdsError("label_repository_changed", repository_id=repository_id)
        latest(labels, label_id, {"label_id": label_id, "repository_id": repository_id,
                                 "name": data["name"], "color": data.get("color"),
                                 "description": data.get("description"), **provenance(raw, priority)})
        return label_id

    def source_fields(raw):
        return {"source_batch_id": str(raw["source_batch_id"]),
                "source_snapshot_id": raw["snapshot_id"],
                "source_observation_id": raw["source_observation_id"],
                "observed_at": raw["observed_at"]}

    batches = {str(batch["batch_id"]): batch for batch in source["batches"]}
    for raw in source["repositories"]:
        data = raw["raw_json"]
        batch = batches[str(raw["source_batch_id"])]
        known = batch.get("created_since_source") in ("recorded", "attested")
        normalized["repositories"].append({
            "repository_id": raw["repository_id"], "full_name": data["full_name"],
            "html_url": data.get("html_url"), **source_fields(raw),
            "source_batch_started_at": _time(batch["started_at"], "batch.started_at"),
            "source_batch_finished_at": _time(batch["finished_at"], "batch.finished_at"),
            "cohort_created_since": _time(batch["created_since"], "batch.created_since") if known else None,
            "cohort_source": batch["created_since_source"] if known else None,
            "cohort_evidence": batch.get("created_since_evidence") if known else None,
            "verified_issue_count": batch["issue_scan_issue_count"],
        })
    events_by_issue = defaultdict(list)
    for raw in source["events"]:
        data = raw["raw_json"]
        label_data = data.get("label")
        label_id = label(label_data, raw["repository_id"], raw, 0, historical=True) if label_data else None
        events_by_issue[raw["issue_id"]].append({
            "event_id": raw["event_id"], "issue_id": raw["issue_id"],
            "repository_id": raw["repository_id"], "event_type": raw["event_type"],
            "github_created_at": raw["github_created_at"],
            "actor_id": user(data.get("actor"), raw, 0),
            "assignee_id": user(data.get("assignee"), raw, 0),
            "assigner_id": user(data.get("assigner"), raw, 0),
            "target_label_id": label_id,
            "label_name": label_data.get("name") if label_data else None,
            "label_color": label_data.get("color") if label_data else None,
            **source_fields(raw),
        })
    for raw in source["issues"]:
        data = raw["raw_json"]
        issue_id, repository_id = raw["issue_id"], raw["repository_id"]
        checked = source["histories"][issue_id]
        if not isinstance(data.get("title"), str):
            raise DdsError("invalid_issue_title", issue_id=issue_id)
        issue = {
            "issue_id": issue_id, "repository_id": repository_id, "issue_number": raw["issue_number"],
            "author_id": user(data.get("user"), raw, 1), "title": data["title"],
            "html_url": data.get("html_url"), "created_at": _time(data["created_at"], "created_at"),
            "updated_at": _time(data["updated_at"], "updated_at"), "source_state": data["state"],
            "state_reason": data.get("state_reason"),
            "source_closed_at": _time(data["closed_at"], "closed_at") if data.get("closed_at") else None,
            "history_batch_id": str(checked["batch_id"]), **source_fields(raw),
            "history_started_at": _time(checked["started_at"], "history.started_at"),
            "history_finished_at": _time(checked["finished_at"], "history.finished_at"),
            "history_mode": checked["mode"],
            "history_issue_updated_at": _time(checked["issue_updated_at"], "history.issue_updated_at"),
        }
        current_labels = data.get("labels", [])
        assignees = data.get("assignees", [data["assignee"]] if data.get("assignee") else [])
        if not isinstance(current_labels, list) or not isinstance(assignees, list):
            raise DdsError("invalid_current_links", issue_id=issue_id)
        normalized["links"][issue_id] = {
            "labels": sorted({label(item, repository_id, raw, 1) for item in current_labels}),
            "assignees": sorted({user(item, raw, 1) for item in assignees if item is not None}),
        }
        history = build_history(issue, events_by_issue[issue_id])
        normalized["histories"][issue_id] = history
        normalized["warnings"].extend(history["warnings"])
        normalized["issues"].append(issue)
    normalized["users"] = list(users.values())
    normalized["labels"] = list(labels.values())
    return normalized


def _summary(normalized, batch_ids):
    return {"source_batch_ids": [str(value) for value in batch_ids], "rules_version": RULES_VERSION,
            "issues": len(normalized["issues"]),
            "events": sum(len(history["events"]) for history in normalized["histories"].values()),
            "versions": sum(len(history["versions"]) for history in normalized["histories"].values()),
            "warnings": len(normalized["warnings"])}


def initialize_dds():
    connection = store.connect()
    try:
        store.acquire_lock(connection)
        store.apply_schema(connection)
    finally:
        connection.close()  # Session locks are released even on an exception.


def _recover_commit(run_id):
    """A lost COMMIT response does not establish that the transaction failed."""
    connection = store.connect()
    try:
        with connection:
            found = store.rows(connection, "SELECT status,result FROM dds.load_runs WHERE run_id=%s FOR UPDATE",
                               (run_id,))
            return found[0] if found else None
    finally:
        connection.close()


def load_dds(batch_ids: list[str], *, expected_repositories=None, expected_created_since=None) -> dict:
    """Publish explicit batches; optional expectations come from the STG summary."""
    connection = store.connect()
    run_id = str(uuid4())
    started = committing = False
    problems = []
    try:
        store.acquire_lock(connection)
        store.apply_schema(connection)
        store.start_run(connection, run_id, batch_ids)
        started = True
        connection.set_session(isolation_level="REPEATABLE READ", readonly=False, autocommit=False)
        source = read_source(connection, batch_ids)
        if expected_repositories is not None:
            actual = {str(batch["batch_id"]): batch["repository_full_name"] for batch in source["batches"]}
            if actual != expected_repositories:
                raise DdsError("stg_summary_repository_mismatch")
        if expected_created_since is not None:
            actual_since = {str(batch["batch_id"]): batch["created_since"] for batch in source["batches"]}
            if set(actual_since) != set(expected_created_since) or any(
                    actual_since[batch_id] is None or _time(actual_since[batch_id], "batch.created_since")
                    != _time(expected_created_since[batch_id], "summary.created_since")
                    for batch_id in actual_since):
                raise DdsError("stg_summary_cohort_mismatch")
        store.check_publication(connection, source)
        normalized = normalize(source)
        problems = normalized["warnings"]
        store.publish(connection, normalized, run_id)
        result = _summary(normalized, batch_ids) | {"status": "succeeded", "run_id": run_id}
        # The validator requires the publication status in this same transaction.
        store.record_outcome(connection, run_id, "succeeded", problems, result=result)
        checked = validate_current(connection, batch_ids, expected=normalized)
        result.update(coverage_complete=checked["coverage_complete"], repositories=checked["repositories"],
                      ready_for_cdm=checked["ready_for_cdm"], manifest=checked["manifest"])
        with connection.cursor() as cursor:
            cursor.execute("UPDATE dds.load_runs SET result=%s WHERE run_id=%s",
                           (store.json_value(result), run_id))
        committing = True
        connection.commit()
        return result
    except Exception as exc:
        if committing:
            # Discard the possibly broken session before checking the durable outcome.
            connection.close()
            try:
                outcome = _recover_commit(run_id)
            except Exception:
                raise DdsError("commit_outcome_unknown", details={"run_id": run_id}) from exc
            if outcome and outcome["status"] == "succeeded":
                return outcome["result"]
        else:
            try:
                connection.rollback()
            except Exception:
                connection.close()
        if started:
            problem = exc.problem if isinstance(exc, DdsError) else {
                "code": "dds_processing_failed", "severity": "error", "details": {"type": type(exc).__name__}}
            # A separate connection survives rollback and never rewrites success.
            audit = store.connect()
            try:
                with audit:
                    store.record_outcome(audit, run_id, "failed", problems + [problem],
                                         error_code=problem["code"])
            finally:
                audit.close()
        raise
    finally:
        connection.close()


def preview_dds(batch_ids: list[str]) -> dict:
    connection = store.connect()
    try:
        connection.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
        source = read_source(connection, batch_ids, preview=True)
        normalized = normalize(source)
        return _summary(normalized, batch_ids) | {
            "status": "preview", "published": False, "coverage_complete": False,
            "ready_for_cdm": False, "quality_issues": normalized["warnings"],
            "cohort": [{"repository": repository["full_name"],
                        "batch_id": repository["source_batch_id"],
                        "created_since": repository["cohort_created_since"].isoformat()
                        if repository["cohort_created_since"] else None,
                        "source": repository["cohort_source"]}
                       for repository in normalized["repositories"]],
        }
    finally:
        connection.rollback()
        connection.close()


def validate_dds(*, expected_batch_ids: list[str] | None = None) -> dict:
    connection = store.connect()
    try:
        connection.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
        batch_ids = expected_batch_ids
        if batch_ids is None:
            batch_ids = [str(row["source_batch_id"]) for row in store.rows(
                connection, "SELECT source_batch_id FROM dds.repositories ORDER BY full_name")]
        normalized = normalize(read_source(connection, batch_ids)) if batch_ids else None
        return validate_current(connection, expected_batch_ids, expected=normalized)
    finally:
        connection.rollback()
        connection.close()

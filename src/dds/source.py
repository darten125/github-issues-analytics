"""Read confirmed STG observations without changing connection transaction state."""

from datetime import datetime, timezone
from uuid import UUID

from src.stg.config import DEFAULT_CREATED_SINCE, REPOSITORIES

from . import DdsError


def _time(value, field):
    try:
        result = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace("Z", "+00:00"))
        if result.tzinfo is None or result.utcoffset() is None:
            raise ValueError("timezone required")
        return result.astimezone(timezone.utc)
    except (ValueError, TypeError, AttributeError):
        raise DdsError("invalid_timestamp", details={"field": field}) from None


def _id(value, field):
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise DdsError("invalid_identifier", details={"field": field})
    return value


def _require(condition, code, **context):
    if not condition:
        raise DdsError(code, **context)


def _rows(cursor, query, parameters):
    cursor.execute(query, parameters)
    return [{key: str(value) if isinstance(value, UUID) else value
             for key, value in dict(row).items()} for row in cursor.fetchall()]


def _payload(row):
    payload = row.get("raw_json")
    _require(isinstance(payload, dict), "invalid_payload")
    return payload


def _observation(row, cutoff):
    _id(row["snapshot_id"], "snapshot_id")
    _id(row["source_observation_id"], "source_observation_id")
    _require(_time(row["observed_at"], "observed_at") <= cutoff,
             "observation_after_batch")


def project_created_since():
    return _time(DEFAULT_CREATED_SINCE, "created_since")


def _cohort(batch, preview):
    """The boundary is the recorded load parameter, never the oldest Issue found."""
    boundary = batch.get("created_since")
    known = batch.get("created_since_source") in ("recorded", "attested") and boundary is not None
    if not known:
        _require(preview, "cohort_boundary_unknown", details={"batch_id": str(batch["batch_id"])})
        return project_created_since()
    boundary = _time(boundary, "batch.created_since")
    _require(preview or boundary == project_created_since(), "cohort_boundary_mismatch",
             details={"batch_id": str(batch["batch_id"]), "created_since": boundary.isoformat()})
    return boundary


def read_source(connection, batch_ids: list[str], *, preview=False) -> dict:
    """Return explicit batches and their latest eligible repository/issue/events.

    The caller owns transaction isolation and lifetime. A succeeded history load
    is evidence in its own right, even if the surrounding STG batch later failed.
    Business dates may not exceed the selected batch's finished_at.
    """
    _require(isinstance(batch_ids, (list, tuple)) and 1 <= len(batch_ids) <= 3,
             "invalid_batch_selection")
    try:
        requested = [str(UUID(str(value))) for value in batch_ids]
    except (ValueError, TypeError, AttributeError):
        raise DdsError("invalid_batch_id") from None
    _require(len(set(requested)) == len(requested), "duplicate_batch_id")
    from psycopg2.extras import RealDictCursor

    result: dict = {"batches": [], "repositories": [], "issues": [], "events": [], "histories": {}}
    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        batches = _rows(cursor, "SELECT * FROM stg.load_batches WHERE batch_id = ANY(%s::uuid[])", (requested,))
        _require(len(batches) == len(requested), "batch_not_found")
        batch_by_id = {str(row["batch_id"]): row for row in batches}
        _require(set(batch_by_id) == set(requested), "batch_not_found")
        selected_repositories = set()
        for batch_id in requested:
            batch = batch_by_id[batch_id]
            name = batch["repository_full_name"]
            _require(name in REPOSITORIES, "repository_out_of_scope")
            _require(name not in selected_repositories, "duplicate_repository")
            selected_repositories.add(name)
            _require(batch["status"] == "succeeded" and batch["finished_at"] is not None,
                     "batch_not_succeeded")
            cutoff = _time(batch["finished_at"], "batch.finished_at")
            _require(_time(batch["started_at"], "batch.started_at") <= cutoff, "invalid_batch_dates")
            scan = batch["issue_scan_status"]
            ready = scan == "verified" and batch["issue_scan_passes"] >= 2
            if preview:
                ready = ready or (scan == "sample" and batch["issue_scan_passes"] >= 1)
            _require(ready, "batch_not_verified")
            boundary = _cohort(batch, preview)
            incomplete = _rows(cursor, """
                SELECT * FROM stg.issue_event_loads
                WHERE batch_id = %s::uuid
                  AND (status <> 'succeeded' OR finished_at IS NULL
                       OR finished_at > %s OR started_at > finished_at)
                """, (batch_id, cutoff))
            _require(not incomplete, "incomplete_batch_histories")
            repositories = _rows(cursor, """
                SELECT DISTINCT ON (r.repository_id) r.*,
                       o.batch_id AS source_batch_id,
                       o.observation_id AS source_observation_id, o.observed_at
                FROM stg.repository_observations o
                JOIN stg.repositories_raw r ON r.snapshot_id = o.snapshot_id
                WHERE o.batch_id = %s::uuid AND o.observed_at <= %s
                ORDER BY r.repository_id, o.observed_at DESC,
                         o.observation_id DESC, r.snapshot_id DESC
                """, (batch_id, cutoff))
            _require(len(repositories) == 1, "repository_snapshot_count")
            repository = repositories[0]
            payload = _payload(repository)
            repository_id = _id(repository["repository_id"], "repository_id")
            _require(_id(payload.get("id"), "repository.id") == repository_id
                     and repository["full_name"] == payload.get("full_name") == name,
                     "repository_context_mismatch", repository_id=repository_id)
            _observation(repository, cutoff)
            issues = _rows(cursor, """
                SELECT DISTINCT ON (i.issue_id) i.*,
                       o.batch_id AS source_batch_id,
                       o.observation_id AS source_observation_id, o.observed_at
                FROM stg.issue_observations o
                JOIN stg.issues_raw i ON i.snapshot_id = o.snapshot_id
                WHERE o.batch_id = %s::uuid AND o.observed_at <= %s
                ORDER BY i.issue_id, o.observed_at DESC,
                         o.observation_id DESC, i.snapshot_id DESC
                """, (batch_id, cutoff))
            _require(len(issues) == batch["issue_scan_issue_count"], "issue_count_mismatch",
                     repository_id=repository_id)
            issue_by_id = {}
            for issue in issues:
                data = _payload(issue)
                issue_id = _id(issue["issue_id"], "issue_id")
                number = _id(issue["issue_number"], "issue_number")
                _require(issue_id not in issue_by_id, "duplicate_issue_id", issue_id=issue_id)
                issue_by_id[issue_id] = issue
                _require(_id(data.get("id"), "issue.id") == issue_id
                         and _id(data.get("number"), "issue.number") == number
                         and issue["repository_id"] == repository_id,
                         "issue_context_mismatch", issue_id=issue_id, repository_id=repository_id)
                if "repository_url" in data:
                    expected_url = payload.get("url", f"https://api.github.com/repos/{name}")
                    _require(data["repository_url"] == expected_url, "issue_repository_url_mismatch",
                             issue_id=issue_id, repository_id=repository_id)
                if "repository" in data:
                    _require(isinstance(data["repository"], dict)
                             and data["repository"].get("id") == repository_id,
                             "issue_context_mismatch", issue_id=issue_id)
                _require("pull_request" not in data, "pull_request_in_source", issue_id=issue_id)
                created = _time(data.get("created_at"), "issue.created_at")
                updated = _time(data.get("updated_at"), "issue.updated_at")
                _require(created >= boundary and updated >= created,
                         "invalid_issue_dates", issue_id=issue_id)
                _require(updated <= cutoff, "issue_date_after_batch", issue_id=issue_id,
                         details={"field": "updated_at"})
                _require(data.get("state") in ("open", "closed"), "invalid_issue_state", issue_id=issue_id)
                if data.get("closed_at") is not None:
                    closed = _time(data["closed_at"], "issue.closed_at")
                    _require(closed >= created and data["state"] == "closed",
                             "invalid_closed_at", issue_id=issue_id)
                    _require(closed <= cutoff, "issue_date_after_batch", issue_id=issue_id,
                             details={"field": "closed_at"})
                _observation(issue, cutoff)
            ids = list(issue_by_id)
            histories = _rows(cursor, """
                SELECT DISTINCT ON (issue_id) * FROM stg.issue_event_loads
                WHERE issue_id = ANY(%s::bigint[]) AND started_at <= %s
                ORDER BY issue_id, started_at DESC, batch_id DESC
                """, (ids, cutoff))
            _require({row["issue_id"] for row in histories} == set(ids), "missing_issue_history",
                     repository_id=repository_id)
            for history in histories:
                issue = issue_by_id[history["issue_id"]]
                _require(history["repository_id"] == repository_id
                         and history["issue_number"] == issue["issue_number"],
                         "history_context_mismatch", issue_id=issue["issue_id"])
                _require(history["status"] == "succeeded" and history["finished_at"] is not None,
                         "history_not_succeeded", issue_id=issue["issue_id"])
                finished = _time(history["finished_at"], "history.finished_at")
                _require(_time(history["started_at"], "history.started_at") <= finished <= cutoff,
                         "history_outside_batch", issue_id=issue["issue_id"])
                _require(_time(history["issue_updated_at"], "history.issue_updated_at")
                         == _time(issue["raw_json"]["updated_at"], "issue.updated_at"),
                         "stale_issue_history", issue_id=issue["issue_id"])
                result["histories"][issue["issue_id"]] = history
            events = _rows(cursor, """
                SELECT DISTINCT ON (e.event_id) e.*,
                       o.batch_id AS source_batch_id,
                       o.observation_id AS source_observation_id, o.observed_at
                FROM stg.event_observations o
                JOIN stg.issue_events_raw e ON e.snapshot_id = o.snapshot_id
                JOIN stg.issue_event_loads l ON l.batch_id = o.batch_id
                    AND l.issue_id = e.issue_id
                WHERE e.issue_id = ANY(%s::bigint[]) AND l.repository_id = %s
                  AND l.status = 'succeeded' AND l.finished_at IS NOT NULL
                  AND l.started_at <= l.finished_at AND l.finished_at <= %s
                  AND o.observed_at <= %s
                ORDER BY e.event_id, o.observed_at DESC,
                         o.observation_id DESC, e.snapshot_id DESC
                """, (ids, repository_id, cutoff, cutoff))
            for event in events:
                data = _payload(event)
                event_id = _id(event["event_id"], "event_id")
                issue = issue_by_id.get(event["issue_id"])
                _require(issue is not None and event["repository_id"] == repository_id,
                         "event_context_mismatch")
                _require(_id(data.get("id"), "event.id") == event_id
                         and isinstance(event["event_type"], str) and bool(event["event_type"])
                         and data.get("event") == event["event_type"],
                         "event_context_mismatch", issue_id=event["issue_id"])
                event_time = _time(event["github_created_at"], "event.github_created_at")
                _require(event_time == _time(data.get("created_at"), "event.created_at"),
                         "event_timestamp_mismatch", issue_id=event["issue_id"])
                _require(event_time >= _time(issue["raw_json"]["created_at"], "issue.created_at"),
                         "event_before_issue", issue_id=event["issue_id"])
                _require(event_time <= cutoff, "event_after_batch", issue_id=event["issue_id"],
                         details={"event_id": event_id})
                if "issue" in data:
                    _require(isinstance(data["issue"], dict)
                             and data["issue"].get("id") == event["issue_id"]
                             and data["issue"].get("number") == issue["issue_number"],
                             "event_context_mismatch", issue_id=event["issue_id"])
                _observation(event, cutoff)
            result["batches"].append(batch)
            result["repositories"].append(repository)
            result["issues"].extend(issues)
            result["events"].extend(events)
    _require(len({row["issue_id"] for row in result["issues"]}) == len(result["issues"]), "duplicate_issue_id")
    _require(len({row["event_id"] for row in result["events"]}) == len(result["events"]), "duplicate_event_id")
    return result

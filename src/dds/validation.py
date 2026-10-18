"""Validate the current published DDS, including reconstructed history."""

from collections import defaultdict
from uuid import UUID

from src.stg.config import REPOSITORIES

from . import DdsError, RULES_VERSION
from .scd2 import build_history
from .store import FRESHNESS, manifest, rows

# Where each dimension value may come from inside its source snapshot.
USER_FIELDS = {"issue": ("user", "assignee", "assignees"), "event": ("actor", "assignee", "assigner")}
LABEL_FIELDS = {"issue": ("labels",), "event": ("label",)}


def _same(actual, wanted):
    if isinstance(actual, UUID) and isinstance(wanted, str):
        return str(actual) == wanted
    return actual == wanted


def _differs(actual, wanted):
    return any(not _same(actual[key], value) for key, value in wanted.items())


def _grouped(connection, query, issue_ids, *, drop_key=False):
    result = defaultdict(list)
    for row in rows(connection, query, (issue_ids,)):
        result[row.pop("issue_id") if drop_key else row["issue_id"]].append(row)
    return result


def _payload_items(raw, fields):
    for field in fields:
        value = raw.get(field)
        for item in value if isinstance(value, list) else [value]:
            if isinstance(item, dict):
                yield item


def _dimension_rows(connection, table, key, ids):
    """Stored values joined with the exact STG observation and the accepting run."""
    return rows(connection, f"""
        SELECT d.*, l.status AS run_status, l.source_batch_ids AS run_batch_ids,
               (SELECT max(b.finished_at) FROM stg.load_batches b
                WHERE b.batch_id::text IN (SELECT jsonb_array_elements_text(l.source_batch_ids)))
                   AS run_source_finished_at,
               coalesce(io.snapshot_id, eo.snapshot_id) AS observed_snapshot_id,
               coalesce(io.observed_at, eo.observed_at) AS observation_time,
               coalesce(io.batch_id, eo.batch_id) AS observation_batch_id,
               coalesce(ir.raw_json, er.raw_json) AS raw_json,
               coalesce(ir.repository_id, er.repository_id) AS raw_repository_id,
               EXISTS (SELECT 1 FROM stg.issue_event_loads el
                       WHERE el.batch_id = eo.batch_id AND el.issue_id = er.issue_id
                         AND el.status = 'succeeded') AS event_history_succeeded
        FROM dds.{table} d
        LEFT JOIN dds.load_runs l ON l.run_id = d.accepted_run_id
        LEFT JOIN stg.issue_observations io
            ON d.source_kind = 'issue' AND io.observation_id = d.source_observation_id
        LEFT JOIN stg.issues_raw ir ON ir.snapshot_id = io.snapshot_id
        LEFT JOIN stg.event_observations eo
            ON d.source_kind = 'event' AND eo.observation_id = d.source_observation_id
        LEFT JOIN stg.issue_events_raw er ON er.snapshot_id = eo.snapshot_id
        WHERE d.{key} = ANY(%s::bigint[])
        """, (sorted(ids),))


def _validate_dimension(row, key, fields, values, expected):
    """Accepted value must match its own observation and be no older than the input."""
    identifier = row[key]
    code = "published_user_mismatch" if key == "user_id" else "published_label_mismatch"
    if row["accepted_run_id"] is None or row["run_status"] != "succeeded":
        raise DdsError("dimension_provenance_unconfirmed", details={key: identifier})
    if (row["observed_snapshot_id"] is None or row["observed_snapshot_id"] != row["source_snapshot_id"]
            or row["observation_time"] != row["observed_at"] or row["raw_json"] is None):
        raise DdsError("dimension_provenance_mismatch", details={key: identifier})
    if row["source_kind"] == "issue":
        accepted_input = str(row["observation_batch_id"]) in (row["run_batch_ids"] or [])
    else:
        accepted_input = (row["event_history_succeeded"] and row["run_source_finished_at"] is not None
                          and row["observation_time"] <= row["run_source_finished_at"])
    if not accepted_input:
        raise DdsError("dimension_provenance_mismatch", details={key: identifier})
    found = [item for item in _payload_items(row["raw_json"], fields[row["source_kind"]])
             if item.get("id") == identifier]
    if not found or any(row[column] != found[0].get(source) for column, source in values.items()):
        raise DdsError(code, details={key: identifier})
    if key == "label_id" and row["repository_id"] != row["raw_repository_id"]:
        raise DdsError(code, details={key: identifier})
    wanted = expected.get(identifier)
    if wanted is not None:
        stored = tuple(row[column] for column in FRESHNESS)
        candidate = tuple(wanted[column] for column in FRESHNESS)
        if stored < candidate:
            raise DdsError("stale_dimension", details={key: identifier})
        if stored == candidate and _differs(row, wanted):
            raise DdsError(code, details={key: identifier})


def validate_current(connection, expected_batch_ids=None, *, expected=None):
    repositories = rows(connection, "SELECT r.*, l.status AS run_status, l.rules_version "
                                    "FROM dds.repositories r LEFT JOIN dds.load_runs l "
                                    "ON l.run_id = r.current_run_id ORDER BY r.full_name")
    scope = repositories
    if expected_batch_ids is not None:
        try:
            expected_ids = [str(UUID(str(value))) for value in expected_batch_ids]
        except (ValueError, TypeError, AttributeError):
            raise DdsError("invalid_batch_id") from None
        if not 1 <= len(expected_ids) <= 3 or len(set(expected_ids)) != len(expected_ids):
            raise DdsError("invalid_batch_selection")
        scope = [repo for repo in repositories if str(repo["source_batch_id"]) in expected_ids]
        if len(scope) != len(expected_ids):
            raise DdsError("publication_batch_mismatch")
    expected_repositories = {repo["repository_id"]: repo for repo in expected["repositories"]} if expected else {}
    expected_issues = {issue["issue_id"]: issue for issue in expected["issues"]} if expected else None
    for repo in scope:
        if repo["run_status"] != "succeeded":
            raise DdsError("publication_not_succeeded", repository_id=repo["repository_id"])
        if repo["rules_version"] != RULES_VERSION:
            raise DdsError("publication_rules_outdated", repository_id=repo["repository_id"],
                           details={"rules_version": repo["rules_version"]})
        wanted = expected_repositories.get(repo["repository_id"])
        if expected is not None and (wanted is None or _differs(repo, wanted)):
            raise DdsError("published_repository_mismatch", repository_id=repo["repository_id"])

    repository_ids = [repo["repository_id"] for repo in scope]
    issues_by_repository = defaultdict(list)
    for issue in rows(connection, "SELECT * FROM dds.issues WHERE repository_id = ANY(%s::bigint[]) "
                                  "ORDER BY issue_id", (repository_ids,)):
        issues_by_repository[issue["repository_id"]].append(issue)
    issue_ids = [issue["issue_id"] for issues in issues_by_repository.values() for issue in issues]
    events_by_issue = _grouped(connection, "SELECT * FROM dds.issue_events WHERE issue_id = ANY(%s::bigint[]) "
                                           "ORDER BY issue_id, github_created_at, event_id", issue_ids)
    labels_by_issue = _grouped(connection, "SELECT issue_id, label_id FROM dds.issue_current_labels "
                                           "WHERE issue_id = ANY(%s::bigint[]) ORDER BY issue_id, label_id",
                               issue_ids)
    assignees_by_issue = _grouped(connection, "SELECT issue_id, user_id FROM dds.issue_current_assignees "
                                              "WHERE issue_id = ANY(%s::bigint[]) ORDER BY issue_id, user_id",
                                  issue_ids)
    versions_by_issue = _grouped(connection, "SELECT issue_id, status, valid_from, valid_to, is_current, "
                                             "source_event_id FROM dds.issue_status_versions "
                                             "WHERE issue_id = ANY(%s::bigint[]) ORDER BY issue_id, valid_from",
                                 issue_ids, drop_key=True)

    issues_count = events_count = versions_count = 0
    warnings = []
    user_ids: set[int] = set()
    label_ids: set[int] = set()
    for repo in scope:
        issues = issues_by_repository[repo["repository_id"]]
        if len(issues) != repo["verified_issue_count"]:
            raise DdsError("published_issue_count_mismatch", repository_id=repo["repository_id"])
        if expected_issues is not None:
            ids = {issue_id for issue_id, issue in expected_issues.items()
                   if issue["repository_id"] == repo["repository_id"]}
            if {issue["issue_id"] for issue in issues} != ids:
                raise DdsError("published_issue_ids_mismatch", repository_id=repo["repository_id"])
        for issue in issues:
            issue_id = issue["issue_id"]
            if str(issue["source_batch_id"]) != str(repo["source_batch_id"]):
                raise DdsError("issue_publication_mismatch", issue_id=issue_id)
            if issue["history_finished_at"] is None:
                raise DdsError("issue_history_context_missing", issue_id=issue_id)
            events = events_by_issue[issue_id]
            labels = [row["label_id"] for row in labels_by_issue[issue_id]]
            assignees = [row["user_id"] for row in assignees_by_issue[issue_id]]
            if expected_issues is not None:
                if _differs(issue, expected_issues[issue_id]):
                    raise DdsError("published_issue_snapshot_mismatch", issue_id=issue_id)
                expected_events = expected["histories"][issue_id]["events"]
                if len(events) != len(expected_events) or any(
                        _differs(actual, wanted_event) for actual, wanted_event in zip(events, expected_events)):
                    raise DdsError("published_events_mismatch", issue_id=issue_id)
                links = expected["links"][issue_id]
                if labels != links["labels"] or assignees != links["assignees"]:
                    raise DdsError("published_links_mismatch", issue_id=issue_id)
            rebuilt = build_history(issue, events)
            warnings.extend(rebuilt["warnings"])
            for original, computed in zip(events, rebuilt["events"]):
                if any(original[field] != computed[field] for field in (
                        "status_before", "status_after", "is_status_transition")):
                    raise DdsError("event_transition_mismatch", issue_id=issue_id)
            versions = versions_by_issue[issue_id]
            if versions != rebuilt["versions"]:
                raise DdsError("invalid_status_intervals", issue_id=issue_id)
            user_ids.update(value for value in [issue["author_id"], *assignees] if value is not None)
            for event in events:
                user_ids.update(event[column] for column in ("actor_id", "assignee_id", "assigner_id")
                                if event[column] is not None)
                if event["target_label_id"] is not None:
                    label_ids.add(event["target_label_id"])
            label_ids.update(labels)
            issues_count += 1
            events_count += len(events)
            versions_count += len(versions)

    expected_users = {user["user_id"]: user for user in expected["users"]} if expected else {}
    expected_labels = {label["label_id"]: label for label in expected["labels"]} if expected else {}
    for row in _dimension_rows(connection, "users", "user_id", user_ids):
        _validate_dimension(row, "user_id", USER_FIELDS, {"login": "login", "user_type": "type"},
                            expected_users)
    for row in _dimension_rows(connection, "labels", "label_id", label_ids):
        _validate_dimension(row, "label_id", LABEL_FIELDS,
                            {"name": "name", "color": "color", "description": "description"},
                            expected_labels)

    published = manifest(connection, repository_ids)
    return {
        "status": "succeeded", "issues": issues_count, "events": events_count,
        "versions": versions_count, "warnings": len(warnings),
        "coverage_complete": {repo["full_name"] for repo in repositories} == set(REPOSITORIES),
        "ready_for_cdm": bool(published) and all(row["ready_for_cdm"] for row in published),
        "repositories": [{"repository": repo["full_name"],
                          "batch_id": str(repo["source_batch_id"]),
                          "run_id": str(repo["current_run_id"])} for repo in scope],
        "manifest": published,
    }

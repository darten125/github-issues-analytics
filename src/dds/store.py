"""PostgreSQL persistence; transaction boundaries belong to the DDS loader."""

from __future__ import annotations

import json
import os
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from uuid import UUID

from . import DdsError, RULES_VERSION

LOCK_ID = 718230031
PAGE_SIZE = 1000
FRESHNESS = ("observed_at", "source_priority", "source_observation_id")


def connect():
    import psycopg2

    names = ("HOST", "PORT", "NAME", "USER", "PASSWORD")
    missing = [f"DWH_DB_{name}" for name in names if not os.getenv(f"DWH_DB_{name}")]
    if missing:
        raise ValueError("Missing database settings: " + ", ".join(missing))
    return psycopg2.connect(
        host=os.environ["DWH_DB_HOST"], port=os.environ["DWH_DB_PORT"],
        dbname=os.environ["DWH_DB_NAME"], user=os.environ["DWH_DB_USER"],
        password=os.environ["DWH_DB_PASSWORD"], connect_timeout=10,
    )


def json_value(value):
    from psycopg2.extras import Json
    return Json(value, dumps=lambda obj: json.dumps(obj, default=str))


def rows(connection, query, params=()):
    from psycopg2.extras import RealDictCursor
    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]


def acquire_lock(connection):
    # Autocommit is essential: waiting for this lock must precede our snapshot.
    connection.autocommit = True
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_advisory_lock(%s)", (LOCK_ID,))


def apply_schema(connection):
    connection.autocommit = False
    try:
        with connection:
            with connection.cursor() as cursor:
                folder = Path(__file__).resolve().parents[2] / "sql" / "dds"
                for path in sorted(folder.glob("[0-9]*.sql")):
                    cursor.execute(path.read_text(encoding="utf-8"))
    finally:
        connection.autocommit = True


def start_run(connection, run_id, batch_ids):
    with connection.cursor() as cursor:
        cursor.execute(
            "INSERT INTO dds.load_runs (run_id, rules_version, source_batch_ids, status) "
            "VALUES (%s, %s, %s, 'running')",
            (run_id, RULES_VERSION, json_value(batch_ids)),
        )


def record_outcome(connection, run_id, status, problems, result=None, error_code=None):
    from psycopg2.extras import execute_values

    with connection.cursor() as cursor:
        cursor.execute("SELECT status FROM dds.load_runs WHERE run_id=%s FOR UPDATE", (run_id,))
        row = cursor.fetchone()
        if row is None or row[0] != "running":
            return
        execute_values(
            cursor,
            "INSERT INTO dds.quality_issues "
            "(run_id, severity, code, issue_id, repository_id, details) VALUES %s",
            [(run_id, problem["severity"], problem["code"], problem.get("issue_id"),
              problem.get("repository_id"), json_value(problem.get("details", {})))
             for problem in problems],
            page_size=PAGE_SIZE,
        )
        cursor.execute(
            "UPDATE dds.load_runs SET status=%s, result=%s, error_code=%s, "
            "finished_at=clock_timestamp() WHERE run_id=%s AND status='running'",
            (status, json_value(result) if result is not None else None, error_code, run_id),
        )


def upsert(connection, table, records, key, *, dimension=False):
    """Write records in pages; rows whose values did not change are not rewritten.

    All identifiers come from our normalization code, never from CLI input. A
    dimension row is replaced only by an observation at least as fresh, and
    keeps the run that first accepted its current value.
    """
    from psycopg2 import sql
    from psycopg2.extras import execute_values

    if not records:
        return
    columns = list(records[0])
    compared = [column for column in columns if column not in (key, "accepted_run_id")]
    target = sql.Identifier(table)

    def qualified(prefix, names):
        return sql.SQL(", ").join(sql.SQL("{}.{}").format(prefix, sql.Identifier(name)) for name in names)

    condition = sql.SQL("({}) IS DISTINCT FROM ({})").format(
        qualified(target, compared), qualified(sql.SQL("EXCLUDED"), compared))
    if dimension:
        condition = sql.SQL("({}) >= ({}) AND ({} OR {}.accepted_run_id IS NULL)").format(
            qualified(sql.SQL("EXCLUDED"), FRESHNESS), qualified(target, FRESHNESS), condition, target)
    query = sql.SQL("INSERT INTO dds.{} ({}) VALUES %s ON CONFLICT ({}) DO UPDATE SET {} WHERE {}").format(
        target, sql.SQL(", ").join(map(sql.Identifier, columns)), sql.Identifier(key),
        sql.SQL(", ").join(sql.SQL("{0}=EXCLUDED.{0}").format(sql.Identifier(column))
                           for column in columns if column != key),
        condition,
    )
    with connection.cursor() as cursor:
        execute_values(cursor, query, [tuple(record[column] for column in columns) for record in records],
                       page_size=PAGE_SIZE)


def check_publication(connection, source):
    for batch in source["batches"]:
        published = rows(connection,
                         "SELECT b.started_at, b.batch_id FROM dds.repositories r "
                         "JOIN stg.load_batches b ON b.batch_id=r.source_batch_id "
                         "WHERE r.full_name=%s", (batch["repository_full_name"],))
        if published and (batch["started_at"], str(batch["batch_id"])) < (
                published[0]["started_at"], str(published[0]["batch_id"])):
            raise DdsError("older_source_batch", details={"batch_id": str(batch["batch_id"])})
    for repository in source["repositories"]:
        repository_id = repository["repository_id"]
        selected = {issue["issue_id"] for issue in source["issues"]
                    if issue["repository_id"] == repository_id}
        existing = {row["issue_id"] for row in rows(
            connection, "SELECT issue_id FROM dds.issues WHERE repository_id=%s", (repository_id,))}
        if existing - selected:
            raise DdsError("missing_published_issues", repository_id=repository_id,
                           details={"issue_ids": sorted(existing - selected)})


def sync_links(connection, issues, links):
    """Replace current label/assignee sets of all selected Issues at once."""
    issue_ids = [issue["issue_id"] for issue in issues]
    repository = {issue["issue_id"]: issue["repository_id"] for issue in issues}
    labels = [(issue_id, label_id) for issue_id in issue_ids for label_id in links[issue_id]["labels"]]
    assignees = [(issue_id, user_id) for issue_id in issue_ids for user_id in links[issue_id]["assignees"]]
    with connection.cursor() as cursor:
        for table, column, pairs in (("issue_current_labels", "label_id", labels),
                                     ("issue_current_assignees", "user_id", assignees)):
            left = [pair[0] for pair in pairs]
            right = [pair[1] for pair in pairs]
            cursor.execute(
                f"DELETE FROM dds.{table} t WHERE t.issue_id = ANY(%s::bigint[]) AND NOT EXISTS ("
                f"SELECT 1 FROM unnest(%s::bigint[], %s::bigint[]) AS d(issue_id, value) "
                f"WHERE d.issue_id = t.issue_id AND d.value = t.{column})",
                (issue_ids, left, right))
            if table == "issue_current_labels":
                cursor.execute(
                    "INSERT INTO dds.issue_current_labels (issue_id, repository_id, label_id) "
                    "SELECT * FROM unnest(%s::bigint[], %s::bigint[], %s::bigint[]) ON CONFLICT DO NOTHING",
                    (left, [repository[issue_id] for issue_id in left], right))
            else:
                cursor.execute(
                    "INSERT INTO dds.issue_current_assignees (issue_id, user_id) "
                    "SELECT * FROM unnest(%s::bigint[], %s::bigint[]) ON CONFLICT DO NOTHING",
                    (left, right))


def sync_versions(connection, histories):
    """Rewrite only changed intervals; unchanged rows keep their version_id."""
    from psycopg2.extras import execute_values

    fields = ("status", "valid_to", "is_current", "source_event_id")
    existing = {(row["issue_id"], row["valid_from"]): row for row in rows(
        connection, "SELECT * FROM dds.issue_status_versions WHERE issue_id = ANY(%s::bigint[])",
        (list(histories),))}
    desired = {(issue_id, version["valid_from"]): version
               for issue_id, versions in histories.items() for version in versions}
    obsolete = [row["version_id"] for key, row in existing.items() if key not in desired]
    changed = [(existing[key]["version_id"], version) for key, version in desired.items()
               if key in existing and any(existing[key][field] != version[field] for field in fields)]
    added = [(key[0], version) for key, version in desired.items() if key not in existing]
    with connection.cursor() as cursor:
        if obsolete:
            cursor.execute("DELETE FROM dds.issue_status_versions WHERE version_id = ANY(%s::bigint[])",
                           (obsolete,))
        # Close surviving old current rows before introducing the final current row.
        for current in (False, True):
            selected = [(version_id, *(version[field] for field in fields))
                        for version_id, version in changed if version["is_current"] is current]
            if selected:
                execute_values(
                    cursor,
                    "UPDATE dds.issue_status_versions t SET status=d.status, valid_to=d.valid_to, "
                    "is_current=d.is_current, source_event_id=d.source_event_id "
                    "FROM (VALUES %s) AS d(version_id, status, valid_to, is_current, source_event_id) "
                    "WHERE t.version_id = d.version_id",
                    selected, template="(%s::bigint, %s::text, %s::timestamptz, %s::boolean, %s::bigint)",
                    page_size=PAGE_SIZE)
        if added:
            execute_values(
                cursor,
                "INSERT INTO dds.issue_status_versions "
                "(issue_id, status, valid_from, valid_to, is_current, source_event_id) VALUES %s",
                [(issue_id, version["status"], version["valid_from"], version["valid_to"],
                  version["is_current"], version["source_event_id"]) for issue_id, version in added],
                page_size=PAGE_SIZE)


def publish(connection, normalized, run_id):
    issues = normalized["issues"]
    issue_ids = [issue["issue_id"] for issue in issues]
    histories = normalized["histories"]
    events = [event for issue_id in issue_ids for event in histories[issue_id]["events"]]
    repository_of = {issue["issue_id"]: issue["repository_id"] for issue in issues}
    for row in rows(connection, "SELECT issue_id, repository_id FROM dds.issues WHERE issue_id = ANY(%s::bigint[])",
                    (issue_ids,)):
        if row["repository_id"] != repository_of[row["issue_id"]]:
            raise DdsError("issue_repository_changed", issue_id=row["issue_id"])
    labels = {label["label_id"]: label for label in normalized["labels"]}
    for row in rows(connection, "SELECT label_id, repository_id FROM dds.labels WHERE label_id = ANY(%s::bigint[])",
                    (list(labels),)):
        if row["repository_id"] != labels[row["label_id"]]["repository_id"]:
            raise DdsError("label_repository_changed", repository_id=labels[row["label_id"]]["repository_id"])
    selected_events = {event["event_id"]: event["issue_id"] for event in events}
    published = defaultdict(set)
    for row in rows(connection, "SELECT event_id, issue_id FROM dds.issue_events "
                    "WHERE issue_id = ANY(%s::bigint[]) OR event_id = ANY(%s::bigint[])",
                    (issue_ids, list(selected_events))):
        if row["event_id"] in selected_events and selected_events[row["event_id"]] != row["issue_id"]:
            raise DdsError("event_issue_changed", issue_id=selected_events[row["event_id"]])
        published[row["issue_id"]].add(row["event_id"])
    for issue_id in issue_ids:
        if published[issue_id] - {event["event_id"] for event in histories[issue_id]["events"]}:
            raise DdsError("missing_published_events", issue_id=issue_id)

    upsert(connection, "repositories",
           [repository | {"current_run_id": run_id} for repository in normalized["repositories"]],
           "repository_id")
    upsert(connection, "users", [user | {"accepted_run_id": run_id} for user in normalized["users"]],
           "user_id", dimension=True)
    upsert(connection, "labels", [label | {"accepted_run_id": run_id} for label in normalized["labels"]],
           "label_id", dimension=True)
    upsert(connection, "issues", issues, "issue_id")
    upsert(connection, "issue_events", events, "event_id")
    sync_links(connection, issues, normalized["links"])
    sync_versions(connection, {issue_id: histories[issue_id]["versions"] for issue_id in issue_ids})


def _plain(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    return value


def manifest(connection, repository_ids):
    """Compact JSON-safe rows of dds.publication_manifest for the given repositories."""
    return [{key: _plain(value) for key, value in row.items()} for row in rows(
        connection, "SELECT * FROM dds.publication_manifest WHERE repository_id = ANY(%s::bigint[]) "
        "ORDER BY full_name", (list(repository_ids),))]

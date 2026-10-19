"""Atomically publish issue-day facts from one read-only DDS snapshot."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

from . import CdmError, EXPECTED_REPOSITORIES, RULES_VERSION
from . import store

LOG = logging.getLogger(__name__)
UTC = timezone.utc
MICROSECONDS_PER_DAY = Decimal(86_400_000_000)


def _day(value: datetime) -> date:
    return value.astimezone(UTC).date()


def _duration_days(start: datetime, end: datetime) -> Decimal:
    delta = end - start
    micros = (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds
    if micros < 0:
        raise CdmError("close_before_creation")
    return Decimal(micros) / MICROSECONDS_PER_DAY


def _read_source(connection, expected_repositories=None):
    """Every query in this function runs in the caller's read-only RR snapshot."""
    manifest = store.fetch_dicts(connection, """
        SELECT repository_id, full_name, current_run_id, source_batch_id,
               source_batch_finished_at, reporting_horizon,
               last_complete_report_day, ready_for_cdm
        FROM dds.publication_manifest ORDER BY full_name
    """)
    if {row["full_name"] for row in manifest} != set(EXPECTED_REPOSITORIES) or len(manifest) != 3:
        raise CdmError("coverage_incomplete")
    if not all(row["ready_for_cdm"] for row in manifest):
        raise CdmError("dds_not_ready")
    if any(row["last_complete_report_day"] is None for row in manifest):
        raise CdmError("missing_report_horizon")
    if expected_repositories:
        current = {row["full_name"]: (str(row["source_batch_id"]), str(row["current_run_id"]))
                   for row in manifest}
        for expected in expected_repositories:
            name = expected.get("repository")
            if current.get(name) != (str(expected.get("batch_id")), str(expected.get("run_id"))):
                raise CdmError("dds_publication_changed", str(name))
    common_last_day = min(row["last_complete_report_day"] for row in manifest)
    if common_last_day < date(2023, 1, 1):
        raise CdmError("no_complete_cohort_day")
    repository_ids = [row["repository_id"] for row in manifest]
    # The DDS function rejects a future or incomplete day for this exact set.
    checked = store.fetch_dicts(connection,
        "SELECT dds.report_day_end(%s::date, %s::bigint[]) AS cutoff",
        (common_last_day, repository_ids))
    cutoff = checked[0]["cutoff"]
    issues = store.fetch_dicts(connection, """
        SELECT issue_id, repository_id, issue_number, author_id, created_at
        FROM dds.issues WHERE repository_id = ANY(%s::bigint[])
        ORDER BY issue_id
    """, (repository_ids,))
    versions = store.fetch_dicts(connection, """
        SELECT v.issue_id, v.status, v.valid_from, v.valid_to
        FROM dds.issue_status_versions v
        JOIN dds.issues i USING (issue_id)
        WHERE i.repository_id = ANY(%s::bigint[])
        ORDER BY v.issue_id, v.valid_from
    """, (repository_ids,))
    transitions = store.fetch_dicts(connection, """
        SELECT e.issue_id, e.event_id, e.github_created_at, e.status_after
        FROM dds.issue_events e
        WHERE e.repository_id = ANY(%s::bigint[]) AND e.is_status_transition
        ORDER BY e.issue_id, e.github_created_at, e.event_id
    """, (repository_ids,))
    labels = store.fetch_dicts(connection, """
        SELECT cl.issue_id, cl.repository_id, cl.label_id,
               l.name AS label_name, l.color AS label_color
        FROM dds.issue_current_labels cl
        JOIN dds.labels l ON l.label_id = cl.label_id
        WHERE cl.repository_id = ANY(%s::bigint[])
        ORDER BY cl.issue_id, cl.label_id
    """, (repository_ids,))
    payload = [manifest, issues, versions, transitions, labels, [common_last_day, cutoff, RULES_VERSION]]
    fingerprint = hashlib.sha256(json.dumps(payload, sort_keys=True, default=str,
                                             ensure_ascii=False).encode("utf-8")).hexdigest()
    return {
        "manifest": manifest, "issues": issues, "versions": versions,
        "transitions": transitions, "labels": labels,
        "common_last_day": common_last_day, "cutoff": cutoff,
        "fingerprint": fingerprint,
    }


def _per_issue(source):
    versions = defaultdict(list)
    transitions = defaultdict(list)
    for row in source["versions"]:
        versions[row["issue_id"]].append(row)
    for row in source["transitions"]:
        transitions[row["issue_id"]].append(row)
    output = []
    for issue in source["issues"]:
        issue_id = issue["issue_id"]
        history = versions[issue_id]
        if not history or history[0]["valid_from"] != issue["created_at"]:
            raise CdmError("invalid_status_history", str(issue_id))
        closes = [event["github_created_at"] for event in transitions[issue_id]
                  if event["status_after"] == "closed"]
        reopens = [event["github_created_at"] for event in transitions[issue_id]
                   if event["status_after"] == "open"]
        first_close = min(closes) if closes else None
        first_reopen = min(reopens) if reopens else None
        if first_reopen is not None and (first_close is None or first_reopen < first_close):
            raise CdmError("reopen_without_close", str(issue_id))
        duration = _duration_days(issue["created_at"], first_close) if first_close else None
        output.append((issue, history, first_close, first_reopen, duration))
    return output


def _copy_rows(connection, table, columns, values):
    """Stream batches through COPY; None is the explicit CSV NULL marker."""
    count = 0
    with connection.cursor() as cursor:
        buffer = io.StringIO()
        writer = csv.writer(buffer, lineterminator="\n")

        def flush():
            buffer.seek(0)
            cursor.copy_expert(
                f"COPY cdm.{table} ({', '.join(columns)}) "
                "FROM STDIN WITH (FORMAT CSV, NULL '\\N')", buffer)
            buffer.seek(0)
            buffer.truncate(0)

        for row in values:
            writer.writerow(["\\N" if value is None else value for value in row])
            count += 1
            if count % 5000 == 0:
                flush()
        if buffer.tell():
            flush()
    return count


def _days(per_issue, common_last_day):
    for issue, history, first_close, first_reopen, duration in per_issue:
        created_day = _day(issue["created_at"])
        interval_index = 0
        day = created_day
        while day <= common_last_day:
            cutoff = datetime.combine(day + timedelta(days=1), datetime.min.time(), UTC)
            while (interval_index + 1 < len(history)
                   and history[interval_index + 1]["valid_from"] < cutoff):
                interval_index += 1
            interval = history[interval_index]
            if (interval["valid_from"] >= cutoff
                    or (interval["valid_to"] is not None and interval["valid_to"] < cutoff)):
                raise CdmError("status_missing_at_cutoff", f"{issue['issue_id']} {day}")
            ever_closed = first_close is not None and first_close < cutoff
            ever_reopened = first_reopen is not None and first_reopen < cutoff
            yield (issue["issue_id"], day, issue["repository_id"], interval["status"],
                   day == created_day, ever_closed, ever_reopened,
                   duration if ever_closed else None)
            day += timedelta(days=1)


def _expected_day_count(source):
    return sum(max(0, (source["common_last_day"] - _day(issue["created_at"])).days + 1)
               for issue in source["issues"])


def _counts(connection):
    return store.fetch_dicts(connection, """
        SELECT (SELECT count(*) FROM cdm.repository_snapshot) AS repositories,
               (SELECT count(*) FROM cdm.issue_lifecycle) AS issues,
               (SELECT count(*) FROM cdm.issue_day) AS issue_days,
               (SELECT count(*) FROM cdm.current_issue_label) AS labels,
               (SELECT count(*) FROM cdm.issue_day WHERE is_new) AS new_issue_days
    """)[0]


def _check_counts(connection, source):
    actual = _counts(connection)
    expected = {
        "repositories": 3,
        "issues": len(source["issues"]),
        "issue_days": _expected_day_count(source),
        "labels": len(source["labels"]),
        "new_issue_days": sum(_day(issue["created_at"]) <= source["common_last_day"]
                              for issue in source["issues"]),
    }
    if actual != expected:
        raise CdmError("cdm_count_mismatch", f"expected={expected}, actual={actual}")
    return actual


def initialize_cdm():
    connection = store.connect()
    try:
        store.lock_dds(connection)
        store.apply_schema(connection)
        return {"status": "schema_ready", "reader_ready": store.ensure_reader(connection)}
    finally:
        connection.close()


def load_cdm(*, expected_repositories=None):
    connection = store.connect()
    run_id = str(uuid4())
    try:
        store.lock_dds(connection)
        store.apply_schema(connection)
        store.ensure_reader(connection)
        connection.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
        source = _read_source(connection, expected_repositories)
        per_issue = _per_issue(source)
        connection.rollback()  # End the DDS-only, read-only snapshot.

        connection.set_session(isolation_level="READ COMMITTED", readonly=False, autocommit=False)
        previous = store.fetch_dicts(connection,
            "SELECT source_fingerprint FROM cdm.current_state WHERE singleton")
        if previous and previous[0]["source_fingerprint"] == source["fingerprint"]:
            counts = _check_counts(connection, source)
            with connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO cdm.load_runs
                        (run_id, status, finished_at, source_fingerprint, result)
                    VALUES (%s, 'unchanged', clock_timestamp(), %s, %s::jsonb)
                """, (run_id, source["fingerprint"], json.dumps(counts)))
            connection.commit()
            return {"status": "unchanged", "run_id": run_id,
                    "common_last_day": source["common_last_day"].isoformat(), **counts}

        with connection.cursor() as cursor:
            cursor.execute("INSERT INTO cdm.load_runs (run_id, status, source_fingerprint) "
                           "VALUES (%s, 'succeeded', %s)", (run_id, source["fingerprint"]))
            for table in ("current_issue_label", "issue_day", "issue_lifecycle", "repository_snapshot"):
                cursor.execute(f"DELETE FROM cdm.{table}")
        _copy_rows(connection, "repository_snapshot", (
            "repository_id", "full_name", "source_batch_id", "dds_run_id",
            "source_batch_finished_at", "reporting_horizon", "last_complete_report_day",
            "common_last_day"), (
            (row["repository_id"], row["full_name"], row["source_batch_id"],
             row["current_run_id"], row["source_batch_finished_at"], row["reporting_horizon"],
             row["last_complete_report_day"], source["common_last_day"])
            for row in source["manifest"]))
        _copy_rows(connection, "issue_lifecycle", (
            "issue_id", "repository_id", "issue_number", "author_id", "created_at",
            "first_closed_at", "first_closed_day", "first_reopened_at", "first_close_days"), (
            (issue["issue_id"], issue["repository_id"], issue["issue_number"],
             issue["author_id"], issue["created_at"], first_close,
             _day(first_close) if first_close else None, first_reopen, duration)
            for issue, _, first_close, first_reopen, duration in per_issue))
        day_count = _copy_rows(connection, "issue_day", (
            "issue_id", "report_day", "repository_id", "status_eod", "is_new",
            "ever_closed", "ever_reopened", "first_close_days"),
            _days(per_issue, source["common_last_day"]))
        _copy_rows(connection, "current_issue_label", (
            "issue_id", "repository_id", "label_id", "label_name", "label_color"), (
            (row["issue_id"], row["repository_id"], row["label_id"],
             row["label_name"], row["label_color"]) for row in source["labels"]))
        if day_count != _expected_day_count(source):
            raise CdmError("issue_day_generation_mismatch")
        counts = _check_counts(connection, source)
        with connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO cdm.current_state
                    (singleton, run_id, source_fingerprint, common_last_day)
                VALUES (TRUE, %s, %s, %s)
                ON CONFLICT (singleton) DO UPDATE SET
                    run_id = EXCLUDED.run_id,
                    source_fingerprint = EXCLUDED.source_fingerprint,
                    common_last_day = EXCLUDED.common_last_day,
                    published_at = clock_timestamp()
            """, (run_id, source["fingerprint"], source["common_last_day"]))
            cursor.execute("UPDATE cdm.load_runs SET finished_at=clock_timestamp(), result=%s::jsonb "
                           "WHERE run_id=%s", (json.dumps(counts), run_id))
        connection.commit()
        return {"status": "succeeded", "run_id": run_id,
                "common_last_day": source["common_last_day"].isoformat(), **counts}
    except Exception as exc:
        try:
            connection.rollback()
            connection.set_session(isolation_level="READ COMMITTED", readonly=False,
                                   autocommit=False)
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO cdm.load_runs (run_id, status, finished_at, error_code)
                        VALUES (%s, 'failed', clock_timestamp(), %s)
                    """, (run_id, exc.code if isinstance(exc, CdmError) else type(exc).__name__))
        except Exception:
            LOG.exception("Could not record failed CDM run")
        raise
    finally:
        connection.close()


def validate_cdm():
    connection = store.connect()
    try:
        connection.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
        source = _read_source(connection)
        state = store.fetch_dicts(connection,
            "SELECT run_id, source_fingerprint, common_last_day FROM cdm.current_state WHERE singleton")
        if not state:
            raise CdmError("cdm_not_published")
        if (state[0]["source_fingerprint"] != source["fingerprint"]
                or state[0]["common_last_day"] != source["common_last_day"]):
            raise CdmError("cdm_source_changed")
        counts = _check_counts(connection, source)
        return {"status": "succeeded", "published_run_id": str(state[0]["run_id"]),
                "common_last_day": source["common_last_day"].isoformat(), **counts}
    finally:
        connection.rollback()
        connection.close()

"""Selection and quality gates without a PostgreSQL or driver dependency."""
import copy
import sys
import types
import unittest
from typing import Any
from datetime import datetime, timezone
from unittest.mock import patch
from uuid import UUID

from src.dds import DdsError
from src.dds.source import read_source


BATCH = "00000000-0000-0000-0000-000000000001"
OLDER = "00000000-0000-0000-0000-000000000002"
TIME = datetime(2024, 1, 2, tzinfo=timezone.utc)
CREATED = "2023-01-01T00:00:00Z"
UPDATED = "2024-01-01T00:00:00Z"
COHORT = datetime(2023, 1, 1, tzinfo=timezone.utc)


def fixture():
    provenance = {"snapshot_id": 1, "source_batch_id": UUID(BATCH),
                  "source_observation_id": 1, "observed_at": TIME}
    repository = {**provenance, "repository_id": 11, "full_name": "ktorio/ktor",
                  "raw_json": {"id": 11, "full_name": "ktorio/ktor"}}
    issue = {**provenance, "issue_id": 22, "repository_id": 11, "issue_number": 3,
             "raw_json": {"id": 22, "number": 3, "state": "open", "created_at": CREATED,
                          "updated_at": UPDATED, "closed_at": None,
                          "repository_url": "https://api.github.com/repos/ktorio/ktor"}}
    return {"batches": [{"batch_id": UUID(BATCH), "repository_full_name": "ktorio/ktor",
                         "started_at": TIME, "finished_at": TIME, "status": "succeeded",
                         "issue_scan_status": "verified", "issue_scan_passes": 2,
                         "issue_scan_issue_count": 1, "created_since": COHORT,
                         "created_since_source": "recorded", "created_since_evidence": None}],
            "repositories": [repository], "issues": [issue], "events": [], "incomplete": [],
            "histories": [{"batch_id": UUID(OLDER), "issue_id": 22, "repository_id": 11,
                           "issue_number": 3, "status": "succeeded", "started_at": TIME,
                           "finished_at": TIME, "issue_updated_at": datetime(2024, 1, 1, tzinfo=timezone.utc),
                           "events_fetched": 0}]}


class Cursor:
    def __init__(self, values):
        self.values = values
        self.queries = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params):
        self.queries.append((sql, params))
        if "FROM stg.load_batches" in sql:
            key = "batches"
        elif "FROM stg.repository_observations" in sql:
            key = "repositories"
        elif "FROM stg.issue_observations" in sql:
            key = "issues"
        elif "FROM stg.event_observations" in sql:
            key = "events"
        elif "DISTINCT ON (issue_id)" in sql:
            key = "histories"
        elif "FROM stg.issue_event_loads" in sql:
            key = "incomplete"
        else:
            raise AssertionError(sql)
        self.result = copy.deepcopy(self.values[key])

    def fetchall(self):
        return self.result


class Connection:
    def __init__(self, values):
        self.result_cursor = Cursor(values)

    def cursor(self, **kwargs):
        return self.result_cursor


class SourceTests(unittest.TestCase):
    def setUp(self):
        self.values = fixture()
        driver = types.ModuleType("psycopg2")
        extras = types.ModuleType("psycopg2.extras")
        extras.RealDictCursor = object
        self.driver_patch = patch.dict(sys.modules, {"psycopg2": driver, "psycopg2.extras": extras})
        self.driver_patch.start()
        self.addCleanup(self.driver_patch.stop)

    def read(self, **kwargs):
        self.connection = Connection(self.values)
        return read_source(self.connection, [BATCH], **kwargs)

    def assert_code(self, code):
        with self.assertRaises(DdsError) as caught:
            self.read()
        self.assertEqual(code, caught.exception.problem["code"])

    def test_previous_history_with_zero_events_and_string_provenance(self):
        result = self.read()
        self.assertEqual([], result["events"])
        self.assertEqual(OLDER, result["histories"][22]["batch_id"])
        self.assertEqual(BATCH, result["issues"][0]["source_batch_id"])
        self.assertEqual(BATCH, result["batches"][0]["batch_id"])
        # No connection transaction/commit APIs are implemented by this double.
        event_sql = self.connection.result_cursor.queries[-1][0]
        self.assertIn("l.status = 'succeeded'", event_sql)
        self.assertIn("l.batch_id = o.batch_id", event_sql)
        self.assertNotIn("load_batches", event_sql)
        self.assertIn("l.finished_at <= %s", event_sql)
        self.assertIn("o.observed_at <= %s", event_sql)

    def test_selection_requires_one_to_three_explicit_unique_uuids(self):
        for value in ([], ["bad"], [BATCH, BATCH], [BATCH] * 4):
            with self.subTest(value=value), self.assertRaises(DdsError):
                read_source(Connection(self.values), value)

    def test_missing_exact_batch_has_no_fallback(self):
        self.values["batches"] = []
        self.assert_code("batch_not_found")

    def test_duplicate_repository_and_out_of_scope_rejected(self):
        self.values["batches"].append({**self.values["batches"][0], "batch_id": UUID(OLDER)})
        with self.assertRaises(DdsError) as caught:
            read_source(Connection(self.values), [BATCH, OLDER])
        self.assertEqual("duplicate_repository", caught.exception.problem["code"])
        self.values = fixture()
        self.values["batches"][0]["repository_full_name"] = "outside/repository"
        self.assert_code("repository_out_of_scope")

    def test_failed_or_unfinished_batch_rejected(self):
        for column, value in (("status", "failed"), ("finished_at", None)):
            with self.subTest(column=column):
                self.values = fixture()
                self.values["batches"][0][column] = value
                self.assert_code("batch_not_succeeded")

    def test_sample_preview_only(self):
        self.values["batches"][0].update(issue_scan_status="sample", issue_scan_passes=1)
        self.assert_code("batch_not_verified")
        self.assertEqual(1, len(self.read(preview=True)["issues"]))

    def test_verified_requires_two_passes_even_preview(self):
        self.values["batches"][0]["issue_scan_passes"] = 1
        with self.assertRaises(DdsError):
            self.read(preview=True)

    def test_incomplete_attempt_in_selected_batch_rejected(self):
        self.values["incomplete"] = [{"status": "running"}]
        self.assert_code("incomplete_batch_histories")

    def test_wrong_repository_payload_rejected(self):
        self.values["repositories"][0]["raw_json"]["id"] = 12
        self.assert_code("repository_context_mismatch")

    def test_issue_count_compares_unique_selected_snapshots(self):
        self.values["batches"][0]["issue_scan_issue_count"] = 2
        self.assert_code("issue_count_mismatch")

    def test_issue_context_and_pr_checks(self):
        mutations = [("id", 23, "issue_context_mismatch"),
                     ("number", 4, "issue_context_mismatch"),
                     ("repository_url", "wrong", "issue_repository_url_mismatch"),
                     ("pull_request", {}, "pull_request_in_source"),
                     ("created_at", "2022-12-31T00:00:00Z", "invalid_issue_dates"),
                     ("created_at", "2023-01-01T00:00:00", "invalid_timestamp"),
                     ("closed_at", "2023-02-01T00:00:00Z", "invalid_closed_at")]
        for column, value, code in mutations:
            with self.subTest(column=column):
                self.values = fixture()
                self.values["issues"][0]["raw_json"][column] = value
                self.assert_code(code)

    def test_latest_history_must_exist_be_successful_and_match_context(self):
        mutations = [("status", "failed", "history_not_succeeded"),
                     ("issue_updated_at", TIME, "stale_issue_history"),
                     ("issue_number", 4, "history_context_mismatch"),
                     ("finished_at", datetime(2025, 1, 1, tzinfo=timezone.utc), "history_outside_batch")]
        for column, value, code in mutations:
            with self.subTest(column=column):
                self.values = fixture()
                self.values["histories"][0][column] = value
                self.assert_code(code)
        self.values["histories"] = []
        self.assert_code("missing_issue_history")

    def test_event_payload_metadata_and_dates(self):
        event = {"snapshot_id": 2, "source_batch_id": UUID(OLDER),
                 "source_observation_id": 2, "observed_at": TIME,
                 "event_id": 33, "issue_id": 22, "repository_id": 11,
                 "event_type": "labeled", "github_created_at": TIME,
                 "raw_json": {"id": 33, "event": "labeled", "created_at": "2024-01-02T00:00:00Z"}}
        self.values["events"] = [event]
        self.assertEqual(OLDER, self.read()["events"][0]["source_batch_id"])
        event["raw_json"]["id"] = 34
        self.assert_code("event_context_mismatch")
        event["raw_json"]["id"] = 33
        event["raw_json"]["created_at"] = "2024-01-01T00:00:00Z"
        self.assert_code("event_timestamp_mismatch")
        event["raw_json"]["created_at"] = "2022-01-01T00:00:00Z"
        event["github_created_at"] = datetime(2022, 1, 1, tzinfo=timezone.utc)
        self.assert_code("event_before_issue")
        event["raw_json"]["created_at"] = "2024-01-02T00:00:00Z"
        event["github_created_at"] = TIME
        event["repository_id"] = 12
        self.assert_code("event_context_mismatch")


    def test_cohort_is_the_recorded_parameter_not_the_oldest_issue(self):
        # A 2026 load must not pass as the 2023 cohort, even if all Issues are recent.
        self.values["batches"][0]["created_since"] = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.values["issues"][0]["raw_json"].update(created_at="2026-02-01T00:00:00Z",
                                                    updated_at="2026-02-01T00:00:00Z")
        self.values["histories"][0]["issue_updated_at"] = datetime(2026, 2, 1, tzinfo=timezone.utc)
        for batch_time in ("started_at", "finished_at"):
            self.values["batches"][0][batch_time] = datetime(2026, 3, 1, tzinfo=timezone.utc)
        for row in (self.values["repositories"][0], self.values["issues"][0]):
            row["observed_at"] = datetime(2026, 3, 1, tzinfo=timezone.utc)
        for history_time in ("started_at", "finished_at"):
            self.values["histories"][0][history_time] = datetime(2026, 3, 1, tzinfo=timezone.utc)
        self.assert_code("cohort_boundary_mismatch")
        self.assertEqual(1, len(self.read(preview=True)["issues"]))

    def test_unknown_legacy_boundary_requires_attestation(self):
        self.values["batches"][0].update(created_since=None, created_since_source="unknown")
        self.assert_code("cohort_boundary_unknown")
        self.values["batches"][0].update(issue_scan_status="sample", issue_scan_passes=1)
        self.assertEqual(1, len(self.read(preview=True)["issues"]))
        self.values["batches"][0].update(issue_scan_status="verified", issue_scan_passes=2,
                                         created_since=COHORT, created_since_source="attested",
                                         created_since_evidence="DagRun conf")
        self.assertEqual(1, len(self.read()["issues"]))

    def test_business_dates_after_batch_end_are_rejected_and_exact_end_accepted(self):
        for column in ("updated_at", "closed_at"):
            with self.subTest(column=column):
                self.values = fixture()
                data = self.values["issues"][0]["raw_json"]
                if column == "closed_at":
                    data["state"] = "closed"
                data[column] = "2024-01-02T00:00:01Z"
                self.assert_code("issue_date_after_batch")
                data[column] = "2024-01-02T00:00:00Z"
                if column == "updated_at":
                    self.values["histories"][0]["issue_updated_at"] = TIME
                self.assertEqual(1, len(self.read()["issues"]))

    def test_event_after_batch_end_is_rejected_and_exact_end_accepted(self):
        event: dict[str, Any] = {"snapshot_id": 2, "source_batch_id": UUID(OLDER),
                 "source_observation_id": 2, "observed_at": TIME,
                 "event_id": 33, "issue_id": 22, "repository_id": 11,
                 "event_type": "labeled", "github_created_at": TIME,
                 "raw_json": {"id": 33, "event": "labeled", "created_at": "2024-01-02T00:00:00Z"}}
        self.values["events"] = [event]
        self.assertEqual(1, len(self.read()["events"]))
        event["github_created_at"] = datetime(2024, 1, 2, 0, 0, 1, tzinfo=timezone.utc)
        event["raw_json"]["created_at"] = "2024-01-02T00:00:01Z"
        self.assert_code("event_after_batch")

if __name__ == "__main__":
    unittest.main()

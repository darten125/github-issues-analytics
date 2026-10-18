"""Opt-in PostgreSQL 16 integration tests; deletes only this test's fixtures."""

import copy
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import threading
import unittest
from typing import Any
from unittest.mock import patch
import uuid

COHORT = datetime(2023, 1, 1, tzinfo=timezone.utc)


@unittest.skipUnless(os.getenv("DDS_INTEGRATION") == "1", "set DDS_INTEGRATION=1 explicitly")
class DdsIntegrationTests(unittest.TestCase):
    dsn: str
    driver: Any
    StgStore: Any
    repositories: tuple[str, ...]

    @classmethod
    def setUpClass(cls):
        import psycopg2
        from src.dds.testdb import require_test_database
        from src.stg.config import REPOSITORIES
        from src.stg.store import StgStore

        cls.dsn = os.getenv("DDS_TEST_DSN") or ""
        # Checked before the first connection, regardless of runtime credentials.
        require_test_database(cls.dsn)
        cls.driver = psycopg2
        cls.StgStore = StgStore
        cls.repositories = REPOSITORIES
        connection = psycopg2.connect(cls.dsn)
        try:
            if not 160000 <= connection.server_version < 170000:
                raise RuntimeError("integration tests require PostgreSQL 16")
            StgStore(connection).apply_schema()
            with connection:
                with connection.cursor() as cursor:
                    root = Path(__file__).resolve().parents[2]
                    for path in sorted((root / "sql" / "dds").glob("[0-9]*.sql")):
                        cursor.execute(path.read_text(encoding="utf-8"))
        finally:
            connection.close()

    def setUp(self):
        self.base = 10**12 + uuid.uuid4().int % 10**12
        self.batch_ids = []
        self.repository_ids = set()
        self.issue_ids = set()
        self.user_ids = set()
        self.connection = self.driver.connect(self.dsn)
        self.store = self.StgStore(self.connection)
        self.connection_patch = patch("src.dds.store.connect", side_effect=lambda: self.driver.connect(self.dsn))
        self.connection_patch.start()
        self.addCleanup(self.connection_patch.stop)
        self.addCleanup(self.connection.close)
        self.addCleanup(self.cleanup_fixtures)

    def rows(self, query, parameters=()):
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute(query, parameters)
                return cursor.fetchall() if cursor.description else []

    def scalar(self, query, parameters=()):
        return self.rows(query, parameters)[0][0]

    def payload(self, *, state="open", closed_at=None, updated="2023-02-01T00:00:00Z", **changes):
        selected = {"id": self.base + 10, "number": 1, "title": "integration fixture",
                    "created_at": "2023-01-01T00:00:00Z", "updated_at": updated,
                    "state": state, "closed_at": closed_at, "user": None,
                    "labels": [], "assignees": [], "html_url": "https://github.com/test/issues/1"}
        selected.update(changes)
        return selected

    def event(self, offset, kind, at="2023-01-02T00:00:00Z", **changes):
        selected = {"id": self.base + 100 + offset, "event": kind, "created_at": at, "actor": None}
        selected.update(changes)
        return selected

    def batch(self, payload=None, events=(), *, repository=0, history=True,
              status="succeeded", scan="verified", cohort=COHORT, legacy=False):
        payload = copy.deepcopy(payload if payload is not None else self.payload())
        repository_id = self.base + repository + 1
        self.repository_ids.add(repository_id)
        self.issue_ids.add(payload["id"])
        batch_id = self.store.start_batch(self.repositories[repository], created_since=cohort)
        self.batch_ids.append(batch_id)
        if legacy:
            # A batch written before STG migration 004 has no known boundary.
            self.rows("UPDATE stg.load_batches SET created_since=NULL, created_since_source='unknown' "
                      "WHERE batch_id=%s", (batch_id,))
        self.store.save_repository(batch_id, {"id": repository_id, "full_name": self.repositories[repository],
                                               "html_url": "https://github.com/" + self.repositories[repository]})
        self.store.save_issues(batch_id, repository_id, [payload], fetched_count=1)
        if history:
            self.store.start_issue_events(batch_id, repository_id, payload)
            self.store.save_events(batch_id, repository_id, payload["id"], list(events))
            self.store.finish_issue_events(batch_id, payload["id"], "succeeded")
        self.store.record_issue_scan(batch_id, scan, 2 if scan == "verified" else 1, 1)
        self.store.finish_batch(batch_id, status)
        return batch_id

    def cleanup_fixtures(self):
        """Never truncate shared tables or touch rows outside generated IDs/batches."""
        self.connection.rollback()
        if not self.batch_ids:
            return
        with self.connection:
            with self.connection.cursor() as cursor:
                run_predicate = " OR ".join("source_batch_ids::text LIKE %s" for _ in self.batch_ids)
                cursor.execute("SELECT run_id FROM dds.load_runs WHERE " + run_predicate,
                               ["%" + batch + "%" for batch in self.batch_ids])
                run_ids = [str(row[0]) for row in cursor.fetchall()]
                issues = list(self.issue_ids)
                repositories = list(self.repository_ids)
                for table in ("issue_current_labels", "issue_current_assignees", "issue_status_versions", "issue_events", "issues"):
                    cursor.execute("DELETE FROM dds." + table + " WHERE issue_id = ANY(%s)", (issues,))
                cursor.execute("DELETE FROM dds.labels WHERE repository_id = ANY(%s)", (repositories,))
                cursor.execute("DELETE FROM dds.repositories WHERE repository_id = ANY(%s)", (repositories,))
                cursor.execute("DELETE FROM dds.users WHERE user_id BETWEEN %s AND %s OR user_id = ANY(%s)",
                               (self.base + 1000, self.base + 1100, list(self.user_ids)))
                if run_ids:
                    cursor.execute("DELETE FROM dds.quality_issues WHERE run_id = ANY(%s::uuid[])", (run_ids,))
                    cursor.execute("DELETE FROM dds.load_runs WHERE run_id = ANY(%s::uuid[])", (run_ids,))
                for table in ("repository_observations", "issue_observations", "event_observations", "issue_event_loads"):
                    cursor.execute("DELETE FROM stg." + table + " WHERE batch_id = ANY(%s::uuid[])", (self.batch_ids,))
                for table in ("issue_events_raw", "issues_raw", "repositories_raw"):
                    cursor.execute("DELETE FROM stg." + table + " WHERE repository_id = ANY(%s)", (repositories,))
                cursor.execute("DELETE FROM stg.load_batches WHERE batch_id = ANY(%s::uuid[])", (self.batch_ids,))

    def test_zero_event_history_and_idempotent_version_ids(self):
        from src.dds import load_dds, validate_dds
        batch = self.batch()
        load_dds([batch])
        before = self.rows("SELECT version_id, status, valid_from, valid_to FROM dds.issue_status_versions WHERE issue_id=%s", (self.base + 10,))
        result = load_dds([batch])
        after = self.rows("SELECT version_id, status, valid_from, valid_to FROM dds.issue_status_versions WHERE issue_id=%s", (self.base + 10,))
        self.assertEqual(before, after)
        self.assertEqual("open", after[0][1])
        self.assertFalse(result["coverage_complete"])
        validate_dds(expected_batch_ids=[batch])

    def test_reuses_successful_history_from_failed_parent_batch(self):
        from src.dds import load_dds
        previous = self.batch(status="failed")
        current = self.batch(history=False)
        load_dds([current])
        self.assertEqual(previous, str(self.scalar("SELECT history_batch_id FROM dds.issues WHERE issue_id=%s", (self.base + 10,))))

    def test_late_event_rebuild_preserves_unchanged_interval_id(self):
        from src.dds import load_dds
        close = self.event(3, "closed", "2023-01-04T00:00:00Z")
        payload = self.payload(state="closed", closed_at=close["created_at"])
        first = self.batch(payload, [close])
        load_dds([first])
        initial_id = self.scalar("SELECT version_id FROM dds.issue_status_versions WHERE issue_id=%s ORDER BY valid_from LIMIT 1", (payload["id"],))
        second = self.batch(payload, [self.event(1, "closed"), self.event(2, "reopened", "2023-01-03T00:00:00Z"), close])
        load_dds([second])
        versions = self.rows("SELECT version_id,status FROM dds.issue_status_versions WHERE issue_id=%s ORDER BY valid_from", (payload["id"],))
        self.assertEqual(["open", "closed", "open", "closed"], [row[1] for row in versions])
        self.assertEqual(initial_id, versions[0][0])

    def test_old_batch_is_rejected_and_expected_batch_checks_current_publication(self):
        from src.dds import DdsError, load_dds, validate_dds
        first = self.batch()
        load_dds([first])
        second = self.batch(self.payload(title="newer title"))
        load_dds([second])
        with self.assertRaises(DdsError):
            load_dds([first])
        with self.assertRaises(DdsError):
            validate_dds(expected_batch_ids=[first])
        validate_dds(expected_batch_ids=[second])
        self.assertEqual("newer title", self.scalar("SELECT title FROM dds.issues WHERE issue_id=%s", (self.base + 10,)))

    def test_sample_preview_does_not_write_any_dds_rows(self):
        from src.dds import DdsError, load_dds, preview_dds
        sample = self.batch(scan="sample")
        before = self.rows("SELECT (SELECT count(*) FROM dds.load_runs), (SELECT count(*) FROM dds.issues), (SELECT count(*) FROM dds.quality_issues)")
        preview_dds([sample])
        self.assertEqual(before, self.rows("SELECT (SELECT count(*) FROM dds.load_runs), (SELECT count(*) FROM dds.issues), (SELECT count(*) FROM dds.quality_issues)"))
        with self.assertRaises(DdsError):
            load_dds([sample])

    def test_current_links_are_replaced_and_missing_users_are_allowed(self):
        from src.dds import load_dds
        users = [{"id": self.base + 1000 + n, "login": "u" + str(n), "type": "User"} for n in range(2)]
        labels = [{"id": self.base + 2000 + n, "name": "label" + str(n), "color": "ffffff"} for n in range(2)]
        first = self.batch(self.payload(labels=labels, assignees=users))
        load_dds([first])
        self.assertEqual(2, self.scalar("SELECT count(*) FROM dds.issue_current_labels WHERE issue_id=%s", (self.base + 10,)))
        self.assertEqual(2, self.scalar("SELECT count(*) FROM dds.issue_current_assignees WHERE issue_id=%s", (self.base + 10,)))
        second = self.batch(self.payload(labels=[labels[1]], assignees=[users[1]]))
        load_dds([second])
        self.assertEqual([(labels[1]["id"],)], self.rows("SELECT label_id FROM dds.issue_current_labels WHERE issue_id=%s", (self.base + 10,)))
        self.assertEqual([(users[1]["id"],)], self.rows("SELECT user_id FROM dds.issue_current_assignees WHERE issue_id=%s", (self.base + 10,)))
        self.assertIsNone(self.scalar("SELECT author_id FROM dds.issues WHERE issue_id=%s", (self.base + 10,)))

    def test_conversion_and_simultaneous_events_keep_actual_transitions(self):
        from src.dds import load_dds
        events = [self.event(1, "closed"), self.event(2, "reopened"),
                  self.event(3, "converted_to_discussion", "2023-01-03T00:00:00Z")]
        selected = self.batch(self.payload(state="closed", closed_at=events[-1]["created_at"]), events)
        load_dds([selected])
        self.assertEqual([("open",), ("closed",)], self.rows("SELECT status FROM dds.issue_status_versions WHERE issue_id=%s ORDER BY valid_from", (self.base + 10,)))
        self.assertEqual(3, self.scalar("SELECT count(*) FROM dds.issue_events WHERE issue_id=%s AND is_status_transition", (self.base + 10,)))
        self.assertEqual("converted_to_discussion", self.scalar("SELECT event_type FROM dds.issue_events WHERE event_id=%s", (events[-1]["id"],)))

    def test_later_repository_failure_rolls_back_entire_publication(self):
        from src.dds import DdsError, load_dds
        first = self.batch()
        invalid = self.payload(id=self.base + 11, state="closed")
        second = self.batch(invalid, repository=1)
        with self.assertRaises(DdsError):
            load_dds([first, second])
        self.assertEqual(0, self.scalar("SELECT count(*) FROM dds.repositories WHERE repository_id = ANY(%s)", (list(self.repository_ids),)))
        self.assertEqual(0, self.scalar("SELECT count(*) FROM dds.issues WHERE issue_id = ANY(%s)", (list(self.issue_ids),)))
        self.assertGreater(self.scalar("SELECT count(*) FROM dds.quality_issues WHERE repository_id = ANY(%s)", (list(self.repository_ids),)), 0)

    def test_older_repository_cannot_overwrite_newer_shared_user(self):
        from src.dds import load_dds, validate_dds
        old_user = {"id": self.base + 1000, "login": "old-login", "type": "User"}
        new_user = {**old_user, "login": "new-login"}
        old = self.batch(self.payload(user=old_user))
        new = self.batch(self.payload(id=self.base + 11, user=new_user), repository=1)
        load_dds([new])
        load_dds([old])
        self.assertEqual("new-login", self.scalar("SELECT login FROM dds.users WHERE user_id=%s", (old_user["id"],)))
        # The fresher accepted value of another repository is valid, not stale.
        validate_dds(expected_batch_ids=[old])
        validate_dds()

    def test_changed_event_snapshot_rebuilds_history(self):
        from src.dds import load_dds
        close = self.event(1, "closed")
        first = self.batch(self.payload(state="closed", closed_at=close["created_at"]), [close])
        load_dds([first])
        changed = self.event(1, "closed", "2023-01-03T00:00:00Z")
        second = self.batch(self.payload(state="closed", closed_at=changed["created_at"]), [changed])
        load_dds([second])
        self.assertEqual(1, self.scalar("SELECT count(*) FROM dds.issue_events WHERE issue_id=%s", (self.base + 10,)))
        self.assertEqual("2023-01-03", str(self.scalar("SELECT valid_from::date FROM dds.issue_status_versions WHERE issue_id=%s AND is_current", (self.base + 10,))))

    def test_future_observations_do_not_contaminate_selected_batch(self):
        from src.dds import load_dds
        old = self.batch()
        self.batch(self.payload(state="closed", closed_at="2023-01-02T00:00:00Z"), [self.event(1, "closed")])
        load_dds([old])
        self.assertEqual("open", self.scalar("SELECT source_state FROM dds.issues WHERE issue_id=%s", (self.base + 10,)))
        self.assertEqual(0, self.scalar("SELECT count(*) FROM dds.issue_events WHERE issue_id=%s", (self.base + 10,)))

    def test_stale_reused_history_is_rejected(self):
        from src.dds import DdsError, load_dds
        self.batch()
        selected = self.batch(self.payload(updated="2023-03-01T00:00:00Z"), history=False)
        with self.assertRaises(DdsError) as raised:
            load_dds([selected])
        self.assertEqual("stale_issue_history", raised.exception.problem["code"])
        self.assertEqual(0, self.scalar("SELECT count(*) FROM dds.issues WHERE issue_id=%s", (self.base + 10,)))

    def test_failed_latest_history_does_not_fall_back_to_previous_success(self):
        from src.dds import DdsError, load_dds
        self.batch()
        selected = self.batch()
        self.rows("UPDATE stg.issue_event_loads SET status='failed' WHERE batch_id=%s::uuid", (selected,))
        with self.assertRaises(DdsError):
            load_dds([selected])
        self.assertEqual(0, self.scalar("SELECT count(*) FROM dds.issues WHERE issue_id=%s", (self.base + 10,)))

    def test_write_failure_after_first_issue_restores_both_previous_publications(self):
        from src.dds import load_dds
        from src.dds import store
        first = self.batch()
        second = self.batch(self.payload(id=self.base + 11), repository=1)
        load_dds([first, second])
        before = self.rows("SELECT issue_id,title,source_batch_id FROM dds.issues WHERE issue_id=ANY(%s) ORDER BY issue_id", (list(self.issue_ids),))
        before_repositories = self.rows("SELECT repository_id,source_batch_id,current_run_id FROM dds.repositories WHERE repository_id=ANY(%s) ORDER BY repository_id", (list(self.repository_ids),))
        newer_first = self.batch(self.payload(title="should roll back"))
        newer_second = self.batch(self.payload(id=self.base + 11, title="also roll back"), repository=1)
        real_sync = store.sync_versions
        completed = []

        def fail_after_write(connection, histories):
            real_sync(connection, histories)
            completed.append(sorted(histories))
            with connection.cursor() as cursor:
                # A real FK write failure invalidates the transaction after
                # both issues' rows have already been written.
                cursor.execute("INSERT INTO dds.issue_current_assignees (issue_id,user_id) VALUES (%s,-1)",
                               (self.base + 11,))

        with patch("src.dds.store.sync_versions", side_effect=fail_after_write):
            with self.assertRaises(self.driver.IntegrityError):
                load_dds([newer_first, newer_second])
        self.assertEqual([sorted(self.issue_ids)], completed)
        self.assertEqual(before, self.rows("SELECT issue_id,title,source_batch_id FROM dds.issues WHERE issue_id=ANY(%s) ORDER BY issue_id", (list(self.issue_ids),)))
        self.assertEqual(before_repositories, self.rows("SELECT repository_id,source_batch_id,current_run_id FROM dds.repositories WHERE repository_id=ANY(%s) ORDER BY repository_id", (list(self.repository_ids),)))
        self.assertEqual(1, self.scalar("SELECT count(*) FROM dds.load_runs r JOIN dds.quality_issues q USING(run_id) WHERE r.status='failed' AND r.source_batch_ids ? %s AND q.code='dds_processing_failed'", (newer_first,)))

    def test_waiting_old_publication_observes_new_commit_after_session_lock(self):
        from src.dds import DdsError, load_dds
        from src.dds import store
        old = self.batch()
        new = self.batch(self.payload(title="concurrent newest"))
        new_locked = threading.Event()
        old_waiting = threading.Event()
        old_passed = threading.Event()
        release_new = threading.Event()
        results, errors, idle_after_lock = {}, {}, []
        real_lock = store.acquire_lock

        def ordered_lock(connection):
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute("SET lock_timeout='8s'")
            role = threading.current_thread().name
            if role == "dds-old":
                old_waiting.set()
            real_lock(connection)
            idle_after_lock.append((connection.autocommit, connection.get_transaction_status()))
            if role == "dds-new":
                new_locked.set()
                if not release_new.wait(8):
                    raise RuntimeError("test barrier timed out")
            else:
                old_passed.set()

        def load(role, batch):
            try:
                results[role] = load_dds([batch])
            except Exception as exc:
                errors[role] = exc

        newest = threading.Thread(target=load, args=("new", new), name="dds-new", daemon=True)
        oldest = threading.Thread(target=load, args=("old", old), name="dds-old", daemon=True)
        with patch("src.dds.store.acquire_lock", side_effect=ordered_lock):
            try:
                newest.start()
                self.assertTrue(new_locked.wait(5), "new publication did not acquire lock")
                oldest.start()
                self.assertTrue(old_waiting.wait(5), "old publication did not reach lock")
                self.assertFalse(old_passed.wait(0.2), "session lock failed to serialize publishers")
            finally:
                release_new.set()
                newest.join(12)
                if oldest.ident is not None:
                    oldest.join(12)
        self.assertFalse(newest.is_alive())
        self.assertFalse(oldest.is_alive())
        self.assertNotIn("new", errors)
        self.assertEqual("succeeded", results["new"]["status"])
        self.assertIsInstance(errors.get("old"), DdsError)
        self.assertEqual("older_source_batch", errors["old"].problem["code"])
        self.assertEqual([(True, 0), (True, 0)], idle_after_lock)
        self.assertEqual("concurrent newest", self.scalar("SELECT title FROM dds.issues WHERE issue_id=%s", (self.base + 10,)))

    def test_lost_commit_response_recovers_durable_success(self):
        from src.dds import load_dds
        selected = self.batch()
        driver = self.driver

        class LostCommitResponse:
            def __init__(self, connection):
                object.__setattr__(self, "connection", connection)
                object.__setattr__(self, "raised", False)

            def __getattr__(self, name):
                return getattr(self.connection, name)

            def __setattr__(self, name, value):
                setattr(self.connection, name, value)

            def __enter__(self):
                self.connection.__enter__()
                return self

            def __exit__(self, *arguments):
                return self.connection.__exit__(*arguments)

            def commit(self):
                self.connection.commit()
                object.__setattr__(self, "raised", True)
                raise driver.OperationalError("simulated lost COMMIT response")

        wrapped = LostCommitResponse(driver.connect(self.dsn))
        calls = []

        def connection_factory():
            calls.append(True)
            return wrapped if len(calls) == 1 else driver.connect(self.dsn)

        with patch("src.dds.store.connect", side_effect=connection_factory):
            result = load_dds([selected])
        self.assertTrue(wrapped.raised)
        self.assertEqual("succeeded", result["status"])
        self.assertEqual("succeeded", self.scalar("SELECT status FROM dds.load_runs WHERE run_id=%s", (result["run_id"],)))
        self.assertEqual(0, self.scalar("SELECT count(*) FROM dds.quality_issues WHERE run_id=%s AND severity='error'", (result["run_id"],)))
        self.assertEqual(1, self.scalar("SELECT count(*) FROM dds.issues WHERE issue_id=%s", (self.base + 10,)))

    def test_complete_new_batch_cannot_remove_previously_published_issue(self):
        from src.dds import DdsError, load_dds
        first = self.batch()
        load_dds([first])
        repository_id = self.base + 1
        empty = self.store.start_batch(self.repositories[0], created_since=COHORT)
        self.batch_ids.append(empty)
        self.store.save_repository(empty, {"id": repository_id, "full_name": self.repositories[0]})
        self.store.record_issue_scan(empty, "verified", 2, 0)
        self.store.finish_batch(empty, "succeeded")
        with self.assertRaises(DdsError) as raised:
            load_dds([empty])
        self.assertEqual("missing_published_issues", raised.exception.problem["code"])
        self.assertEqual(first, str(self.scalar("SELECT source_batch_id FROM dds.repositories WHERE repository_id=%s", (repository_id,))))

    def test_all_three_repositories_report_complete_coverage(self):
        from src.dds import load_dds, validate_dds
        batches = [self.batch(self.payload(id=self.base + 10 + index), repository=index) for index in range(3)]
        result = load_dds(batches)
        self.assertTrue(result["coverage_complete"])
        self.assertTrue(validate_dds(expected_batch_ids=batches)["coverage_complete"])
        self.assertEqual(3, self.scalar("SELECT count(*) FROM dds.issues WHERE issue_id=ANY(%s)", (list(self.issue_ids),)))

    def test_standalone_validation_detects_deleted_events_links_and_changed_snapshot(self):
        from src.dds import DdsError, load_dds, validate_dds
        label = {"id": self.base + 2000, "name": "integration-label", "color": "ffffff"}
        user = {"id": self.base + 1000, "login": "integration-user", "type": "User"}
        labeled = self.event(1, "labeled", label={"name": label["name"], "color": label["color"]})
        selected = self.batch(self.payload(labels=[label], assignees=[user]), [labeled])
        load_dds([selected])
        mutations = [
            ("event", "DELETE FROM dds.issue_events WHERE issue_id=%s", "published_events_mismatch"),
            ("label", "DELETE FROM dds.issue_current_labels WHERE issue_id=%s", "published_links_mismatch"),
            ("assignee", "DELETE FROM dds.issue_current_assignees WHERE issue_id=%s", "published_links_mismatch"),
            ("title", "UPDATE dds.issues SET title='tampered' WHERE issue_id=%s", "published_issue_snapshot_mismatch"),
        ]
        for name, query, expected_code in mutations:
            with self.subTest(tampered=name):
                self.rows(query, (self.base + 10,))
                try:
                    with self.assertRaises(DdsError) as raised:
                        validate_dds()
                    self.assertEqual(expected_code, raised.exception.problem["code"])
                finally:
                    load_dds([selected])
                validate_dds()

    def test_commit_failure_before_server_commit_records_failed_without_publication(self):
        from src.dds import load_dds
        selected = self.batch()
        driver = self.driver

        class FailedCommit:
            def __init__(self, connection):
                object.__setattr__(self, "connection", connection)

            def __getattr__(self, name):
                return getattr(self.connection, name)

            def __setattr__(self, name, value):
                setattr(self.connection, name, value)

            def __enter__(self):
                self.connection.__enter__()
                return self

            def __exit__(self, *arguments):
                return self.connection.__exit__(*arguments)

            def commit(self):
                self.connection.rollback()
                raise driver.OperationalError("simulated failure before server COMMIT")

        wrapped = FailedCommit(driver.connect(self.dsn))
        calls = []

        def connection_factory():
            calls.append(True)
            return wrapped if len(calls) == 1 else driver.connect(self.dsn)

        with patch("src.dds.store.connect", side_effect=connection_factory):
            with self.assertRaises(driver.OperationalError):
                load_dds([selected])
        self.assertEqual(0, self.scalar("SELECT count(*) FROM dds.issues WHERE issue_id=%s", (self.base + 10,)))
        self.assertEqual(0, self.scalar("SELECT count(*) FROM dds.repositories WHERE repository_id=%s", (self.base + 1,)))
        self.assertEqual(1, self.scalar("SELECT count(*) FROM dds.load_runs r JOIN dds.quality_issues q USING(run_id) WHERE r.source_batch_ids ? %s AND r.status='failed' AND q.severity='error'", (selected,)))

    def test_airflow_summary_repository_name_must_match_batch_uuid(self):
        from src.dds import DdsError
        from src.dds.airflow_adapter import run_from_stg_summary
        selected = self.batch()
        summary = {"status": "succeeded", "full_load": True,
                   "batches": [{"repository": self.repositories[1], "batch_id": selected}]}
        with self.assertRaises(DdsError):
            run_from_stg_summary(summary)
        self.assertEqual(0, self.scalar("SELECT count(*) FROM dds.repositories WHERE repository_id=%s", (self.base + 1,)))
        self.assertEqual(0, self.scalar("SELECT count(*) FROM dds.issues WHERE issue_id=%s", (self.base + 10,)))

    def test_documented_queries_run_readonly_and_preserve_utc_boundaries(self):
        from src.dds import load_dds
        midnight = "2023-01-02T00:00:00Z"
        first = self.batch(self.payload(state="closed", closed_at=midnight), [self.event(1, "closed", midnight)])
        second_issue_id = self.base + 11
        second = self.batch(self.payload(id=second_issue_id),
                            [self.event(2, "closed", midnight), self.event(3, "reopened", midnight)], repository=1)
        load_dds([first, second])
        path = Path(__file__).resolve().parents[2] / "docs" / "sql" / "DDS_READ_QUERIES.sql"
        executable = "\n".join(line for line in path.read_text(encoding="utf-8").splitlines()
                               if not line.lstrip().startswith("--"))
        queries = [query.strip() for query in executable.split(";") if query.strip()]
        self.assertEqual(8, len(queries))
        queries[0] = queries[0].replace("2024-03-11T22:47:34Z", midnight)
        queries[1] = queries[1].replace("2024-03-11", "2023-01-01")
        connection = self.driver.connect(self.dsn)
        try:
            connection.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
            with connection.cursor() as cursor:
                cursor.execute("SHOW transaction_read_only")
                self.assertEqual("on", cursor.fetchone()[0])
                results = []
                for query in queries:
                    cursor.execute(query)
                    results.append(cursor.fetchall())
            instant = {row[0]: row[3] for row in results[0]}
            end_of_day = {row[0]: row[3] for row in results[1]}
            first_closed = {row[0]: row[3] for row in results[2]}
            self.assertEqual("closed", instant[self.base + 10])
            self.assertEqual("open", end_of_day[self.base + 10])
            self.assertEqual("open", instant[second_issue_id])
            self.assertEqual("2023-01-02T00:00:00+00:00", first_closed[second_issue_id].isoformat())
        finally:
            connection.rollback()
            connection.close()


    def assert_load_code(self, batches, code):
        from src.dds import DdsError, load_dds
        with self.assertRaises(DdsError) as raised:
            load_dds(batches)
        self.assertEqual(code, raised.exception.problem["code"])

    def test_2026_load_is_not_the_2023_cohort_and_legacy_boundary_needs_attestation(self):
        from src.dds import load_dds
        recent = self.payload(created_at="2026-02-01T00:00:00Z", updated="2026-02-02T00:00:00Z")
        self.assert_load_code([self.batch(recent, cohort=datetime(2026, 1, 1, tzinfo=timezone.utc))],
                              "cohort_boundary_mismatch")
        legacy = self.batch(legacy=True)
        self.assert_load_code([legacy], "cohort_boundary_unknown")
        self.assertEqual(0, self.scalar("SELECT count(*) FROM dds.issues WHERE issue_id=%s", (self.base + 10,)))
        evidence = "DagRun manual__stg_full_20261018_0152utc conf"
        self.assertEqual("attested", self.store.attest_created_since(legacy, COHORT, evidence))
        load_dds([legacy])
        self.assertEqual([("attested", evidence, COHORT, True)], self.rows(
            "SELECT cohort_source, cohort_evidence, cohort_created_since, ready_for_cdm "
            "FROM dds.publication_manifest WHERE repository_id=%s", (self.base + 1,)))

    def test_future_business_dates_block_publication(self):
        future = "2999-01-01T00:00:00Z"
        self.assert_load_code([self.batch(self.payload(state="closed", closed_at=future))],
                              "issue_date_after_batch")
        self.assert_load_code([self.batch(self.payload(), [self.event(1, "labeled", future)])],
                              "event_after_batch")
        self.assertEqual(0, self.scalar("SELECT count(*) FROM dds.issues WHERE issue_id=%s", (self.base + 10,)))

    def test_lone_reopened_blocks_and_keeps_previous_publication(self):
        from src.dds import load_dds
        first = self.batch()
        load_dds([first])
        broken = self.batch(self.payload(updated="2023-02-02T00:00:00Z"), [self.event(1, "reopened")])
        self.assert_load_code([broken], "reopened_without_close")
        self.assertEqual(first, str(self.scalar("SELECT source_batch_id FROM dds.repositories WHERE repository_id=%s",
                                                (self.base + 1,))))
        self.assertEqual(1, self.scalar(
            "SELECT count(*) FROM dds.load_runs r JOIN dds.quality_issues q USING (run_id) "
            "WHERE r.status='failed' AND r.source_batch_ids ? %s AND q.code='reopened_without_close'", (broken,)))

    def test_validation_detects_tampered_repository_dimensions_and_provenance(self):
        from src.dds import DdsError, load_dds, validate_dds
        label = {"id": self.base + 2000, "name": "integration-label", "color": "ffffff", "description": None}
        user = {"id": self.base + 1000, "login": "integration-user", "type": "User"}
        selected = self.batch(self.payload(labels=[label], assignees=[user], user=user))
        load_dds([selected])
        cases = [
            ("repositories", "html_url", "'https://example.invalid'", "repository_id", self.base + 1,
             "published_repository_mismatch"),
            ("repositories", "full_name", "'someone/else'", "repository_id", self.base + 1,
             "published_repository_mismatch"),
            ("users", "login", "'mallory'", "user_id", user["id"], "published_user_mismatch"),
            ("labels", "name", "'renamed'", "label_id", label["id"], "published_label_mismatch"),
            ("users", "source_snapshot_id", "source_snapshot_id + 1", "user_id", user["id"],
             "dimension_provenance_mismatch"),
            ("labels", "source_observation_id", "source_observation_id + 1000000", "label_id", label["id"],
             "dimension_provenance_mismatch"),
            ("users", "accepted_run_id", "NULL", "user_id", user["id"], "dimension_provenance_unconfirmed"),
        ]
        for table, column, value, key, identifier, code in cases:
            with self.subTest(table=table, column=column):
                original = self.scalar(f"SELECT {column} FROM dds.{table} WHERE {key}=%s", (identifier,))
                self.rows(f"UPDATE dds.{table} SET {column}={value} WHERE {key}=%s", (identifier,))
                try:
                    with self.assertRaises(DdsError) as raised:
                        validate_dds()
                    self.assertEqual(code, raised.exception.problem["code"])
                finally:
                    restored = str(original) if isinstance(original, uuid.UUID) else original
                    self.rows(f"UPDATE dds.{table} SET {column}=%s WHERE {key}=%s", (restored, identifier))
                validate_dds()

    def test_identical_repeat_keeps_rows_and_version_ids_unwritten(self):
        from src.dds import load_dds
        label = {"id": self.base + 2000, "name": "integration-label", "color": "ffffff", "description": None}
        user = {"id": self.base + 1000, "login": "integration-user", "type": "User"}
        events = [self.event(1, "closed"), self.event(2, "reopened", "2023-01-03T00:00:00Z")]
        selected = self.batch(self.payload(labels=[label], assignees=[user], user=user), events)
        load_dds([selected])
        snapshot = """
            SELECT 'issue', issue_id, xmin::text FROM dds.issues WHERE issue_id=%(issue)s
            UNION ALL SELECT 'event', event_id, xmin::text FROM dds.issue_events WHERE issue_id=%(issue)s
            UNION ALL SELECT 'version', version_id, xmin::text FROM dds.issue_status_versions WHERE issue_id=%(issue)s
            UNION ALL SELECT 'user', user_id, xmin::text FROM dds.users WHERE user_id=%(user)s
            UNION ALL SELECT 'label', label_id, xmin::text FROM dds.labels WHERE label_id=%(label)s
            UNION ALL SELECT 'link', label_id, xmin::text FROM dds.issue_current_labels WHERE issue_id=%(issue)s
            UNION ALL SELECT 'assignee', user_id, xmin::text FROM dds.issue_current_assignees WHERE issue_id=%(issue)s
            ORDER BY 1, 2"""
        parameters = {"issue": self.base + 10, "user": user["id"], "label": label["id"]}
        before = self.rows(snapshot, parameters)
        accepted = self.scalar("SELECT accepted_run_id FROM dds.users WHERE user_id=%s", (user["id"],))
        result = load_dds([selected])
        self.assertEqual(before, self.rows(snapshot, parameters))
        self.assertEqual(10, len(before))
        self.assertEqual(accepted, self.scalar("SELECT accepted_run_id FROM dds.users WHERE user_id=%s", (user["id"],)))
        self.assertEqual(result["run_id"], str(self.scalar(
            "SELECT current_run_id FROM dds.repositories WHERE repository_id=%s", (self.base + 1,))))

    def test_manifest_reads_only_dds_and_restricts_report_days(self):
        from src.dds import load_dds
        first = self.batch()
        second = self.batch(self.payload(id=self.base + 11), repository=1)
        result = load_dds([first, second])
        self.assertEqual({"dds"}, {row[0] for row in self.rows(
            "SELECT DISTINCT table_schema FROM information_schema.view_table_usage "
            "WHERE view_schema='dds' AND view_name='publication_manifest'")})
        manifest = {row["repository_id"]: row for row in result["manifest"]}
        self.assertEqual({self.base + 1, self.base + 2}, set(manifest))
        self.assertTrue(result["ready_for_cdm"])
        for repository_id, batch_id in ((self.base + 1, first), (self.base + 2, second)):
            row = manifest[repository_id]
            self.assertEqual((batch_id, result["run_id"], "2", 1, 1, "recorded", True),
                             (row["source_batch_id"], row["current_run_id"], row["rules_version"],
                              row["verified_issue_count"], row["published_issue_count"],
                              row["cohort_source"], row["ready_for_cdm"]))
        horizons = self.rows("SELECT repository_id, reporting_horizon, last_complete_report_day, "
                             "min_issue_observed_at FROM dds.publication_manifest WHERE repository_id = ANY(%s) "
                             "ORDER BY reporting_horizon", (list(self.repository_ids),))
        (early_id, early, early_day, observed), (late_id, late, _, _) = horizons
        self.assertEqual(observed, early)
        self.assertLess(early, late)
        self.assertEqual(early.astimezone(timezone.utc).date() - timedelta(days=1), early_day)
        both = [early_id, late_id]
        next_midnight = datetime.combine(early_day + timedelta(days=1), datetime.min.time(), timezone.utc)
        self.assertEqual(next_midnight, self.scalar("SELECT dds.report_day_end(%s, %s)", (early_day, both)))
        self.assertEqual(early, self.scalar("SELECT dds.report_cutoff(%s, %s)", (early, both)))
        self.assertEqual(late, self.scalar("SELECT dds.report_cutoff(%s, %s)", (late, [late_id])))
        rejected = [("SELECT dds.report_day_end(%s, %s)", (early_day + timedelta(days=1), both)),
                    ("SELECT dds.report_day_end(%s, %s)", (early_day + timedelta(days=400), both)),
                    ("SELECT dds.report_cutoff(%s, %s)", (late, both)),
                    ("SELECT dds.report_day_end(%s, %s)", (early_day, both + [self.base + 99]))]
        for query, parameters in rejected:
            with self.subTest(parameters=parameters), self.assertRaises(self.driver.DataError):
                self.rows(query, parameters)

    def test_rules_v1_publication_is_kept_but_not_ready_until_republished(self):
        from src.dds import DdsError, load_dds, validate_dds
        from src.dds import store
        user = {"id": self.base + 1000, "login": "integration-user", "type": "User"}
        selected = self.batch(self.payload(user=user))
        result = load_dds([selected])
        self.rows("UPDATE dds.load_runs SET rules_version='1' WHERE run_id=%s", (result["run_id"],))
        # Rules v1 stored neither the source kind/snapshot nor the accepting run.
        self.rows("UPDATE dds.users SET source_kind=NULL, source_snapshot_id=NULL, accepted_run_id=NULL "
                  "WHERE user_id=%s", (user["id"],))
        connection = self.driver.connect(self.dsn)
        try:
            store.apply_schema(connection)
        finally:
            connection.close()
        restored = self.rows("SELECT u.source_kind, u.source_snapshot_id = o.snapshot_id, u.accepted_run_id "
                             "FROM dds.users u JOIN stg.issue_observations o "
                             "ON o.observation_id = u.source_observation_id WHERE u.user_id=%s", (user["id"],))
        self.assertEqual([("issue", True, None)], restored)
        self.assertFalse(self.scalar("SELECT ready_for_cdm FROM dds.publication_manifest WHERE repository_id=%s",
                                     (self.base + 1,)))
        self.assertEqual(1, self.scalar("SELECT count(*) FROM dds.issues WHERE issue_id=%s", (self.base + 10,)))
        with self.assertRaises(DdsError) as raised:
            validate_dds()
        self.assertEqual("publication_rules_outdated", raised.exception.problem["code"])
        republished = load_dds([selected])
        self.assertTrue(republished["ready_for_cdm"])
        self.assertEqual(republished["run_id"], str(self.scalar(
            "SELECT accepted_run_id FROM dds.users WHERE user_id=%s", (user["id"],))))
        validate_dds()

    def test_reference_fixture_reproduces_control_metrics(self):
        from decimal import Decimal
        from src.dds import fixture, load_dds, validate_dds

        self.repository_ids.update(fixture.REPOSITORY_IDS)
        self.issue_ids.update(fixture.ISSUE_IDS)
        self.user_ids.update(user["id"] for user in fixture.USERS.values())
        batch_ids = fixture.build_stg(self.store)
        self.batch_ids.extend(batch_ids)
        result = load_dds(batch_ids)
        self.assertTrue(result["ready_for_cdm"] and result["coverage_complete"])
        self.assertEqual(9, result["issues"])
        validate_dds(expected_batch_ids=batch_ids)
        path = Path(__file__).resolve().parents[2] / "docs" / "sql" / "CDM_REFERENCE_METRICS.sql"
        executable = "\n".join(line for line in path.read_text(encoding="utf-8").splitlines()
                               if not line.lstrip().startswith("--"))
        queries = [query.strip() for query in executable.split(";") if query.strip()]
        self.assertEqual(5, len(queries))
        connection = self.driver.connect(self.dsn)
        try:
            connection.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
            with connection.cursor() as cursor:
                results: list[list[tuple[Any, ...]]] = []
                for query in queries:
                    cursor.execute(query)
                    results.append(cursor.fetchall())
                cursor.execute("SELECT count(*), count(author_id), count(*) FILTER (WHERE author_id=%s) "
                               "FROM dds.issues WHERE issue_id = ANY(%s) AND created_at < dds.report_day_end('2023-03-31')",
                               (fixture.USERS["alice"]["id"], list(fixture.ISSUE_IDS)))
                authors = cursor.fetchone()
                cursor.execute("SELECT count(*), count(DISTINCT issue_id) FROM dds.issue_current_assignees "
                               "WHERE issue_id = ANY(%s)", (list(fixture.ISSUE_IDS),))
                assignees = cursor.fetchone()
                with self.assertRaises(self.driver.DataError):
                    cursor.execute(queries[1].replace("2023-03-31", "2999-01-01"))
        finally:
            connection.rollback()
            connection.close()
        kotlin, ktor, android = self.repositories
        day = datetime(2023, 3, 1).date()
        D = Decimal
        # Database collations order names differently; compare independent of ORDER BY.
        expected: list[list[tuple[Any, ...]]] = [
            [(android, day.replace(day=31), 1), (kotlin, day, 1), (kotlin, day.replace(day=20), 1),
             (ktor, day.replace(day=31), 1)],
            [(android, 2, 0, 2, D("0.0000")), (kotlin, 4, 2, 2, D("0.5000")),
             (ktor, 2, 0, 2, D("0.0000")), ("ALL", 8, 2, 6, D("0.2500"))],
            [(android, 1, D("10.0000"), 0, None), (kotlin, 3, D("10.0000"), 2, D("20.0000")),
             (ktor, 0, None, 0, None), ("ALL", 4, D("10.0000"), 2, D("20.0000"))],
            [(android, 1, 1, D("1.0000")), (kotlin, 3, 1, D("0.3333")), (ktor, 0, 0, None),
             ("ALL", 4, 2, D("0.5000"))],
            [(android, "ui", 1), (kotlin, "bug", 2), (kotlin, "feature", 1), (kotlin, "help", 1),
             (ktor, "bug", 2), (ktor, "feature", 1)],
        ]
        for number, (actual, wanted) in enumerate(zip(results, expected), start=1):
            with self.subTest(query=number):
                self.assertEqual(sorted(wanted), sorted(actual))
        self.assertEqual((8, 7, 2), authors)
        self.assertEqual((6, 3), assignees)

if __name__ == "__main__":
    unittest.main()

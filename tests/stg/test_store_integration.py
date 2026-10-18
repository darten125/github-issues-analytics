import os
import random
import unittest
from datetime import datetime, timezone

from src.stg.loader import load_repository
from src.stg.store import StgStore


@unittest.skipUnless(os.getenv("STG_INTEGRATION") == "1", "requires running analytics PostgreSQL")
class StoreIntegrationTests(unittest.TestCase):
    def test_audit_adds_event_without_issue_update_and_repeat_has_no_new_versions(self):
        store = StgStore.from_env()
        store.apply_schema()
        base = random.randint(8_000_000_000_000_000, 8_999_999_999_000_000)
        batches = []

        class Client:
            def __init__(self, events):
                self.events = events
                self.event_requests = 0

            def get_repository(self, name):
                return {"id": base, "full_name": name}

            def iter_issues(self, name):
                yield [{"id": base + 1, "number": 1, "created_at": "2024-01-01T00:00:00Z",
                        "updated_at": "2024-01-02T00:00:00Z"}]

            def iter_issue_events(self, name, number):
                self.event_requests += 1
                yield self.events

        closed = {"id": base + 2, "event": "closed", "created_at": "2024-01-02T00:00:00Z"}
        reopened = {"id": base + 3, "event": "reopened", "created_at": "2024-01-03T00:00:00Z"}
        try:
            for mode, events, expected_requests in (
                ("backfill", [closed], 1),
                ("sync", [closed, reopened], 0),
                ("audit", [closed, reopened], 1),
                ("audit", [closed, reopened], 1),
            ):
                client = Client(events)
                batches.append(load_repository(client, store, "test/stg-audit",
                                               datetime(2023, 1, 1, tzinfo=timezone.utc),
                                               max_issues=1, mode=mode))
                self.assertEqual(client.event_requests, expected_requests)
            with store.connection.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM stg.issue_events_raw WHERE issue_id = %s", (base + 1,))
                self.assertEqual(cursor.fetchone()[0], 2)
                cursor.execute("SELECT mode, status FROM stg.issue_event_loads WHERE issue_id = %s "
                               "ORDER BY started_at, batch_id", (base + 1,))
                self.assertEqual(cursor.fetchall(),
                                 [("backfill", "succeeded"), ("audit", "succeeded"),
                                  ("audit", "succeeded")])
                cursor.execute("SELECT event_versions_inserted, issue_scan_status, issue_scan_passes "
                               "FROM stg.load_batches WHERE batch_id = %s", (batches[-1],))
                self.assertEqual(cursor.fetchone(), (0, "sample", 1))
        finally:
            store.connection.rollback()
            with store.connection:
                with store.connection.cursor() as cursor:
                    cursor.execute("DELETE FROM stg.issue_event_loads WHERE issue_id = %s", (base + 1,))
                    cursor.execute("DELETE FROM stg.event_observations WHERE snapshot_id IN "
                                   "(SELECT snapshot_id FROM stg.issue_events_raw WHERE issue_id = %s)", (base + 1,))
                    cursor.execute("DELETE FROM stg.issue_observations WHERE snapshot_id IN "
                                   "(SELECT snapshot_id FROM stg.issues_raw WHERE issue_id = %s)", (base + 1,))
                    cursor.execute("DELETE FROM stg.repository_observations WHERE snapshot_id IN "
                                   "(SELECT snapshot_id FROM stg.repositories_raw WHERE repository_id = %s)", (base,))
                    cursor.execute("DELETE FROM stg.issue_events_raw WHERE issue_id = %s", (base + 1,))
                    cursor.execute("DELETE FROM stg.issues_raw WHERE issue_id = %s", (base + 1,))
                    cursor.execute("DELETE FROM stg.repositories_raw WHERE repository_id = %s", (base,))
                    cursor.execute("DELETE FROM stg.load_batches WHERE batch_id = ANY(%s::uuid[])", (batches,))
            store.close()

    def test_sync_changed_then_repeated_unchanged_preserves_versions(self):
        store = StgStore.from_env()
        store.apply_schema()
        base = random.randint(8_000_000_000_000_000, 8_999_999_999_000_000)
        batches = []

        class Client:
            def __init__(self, issue, events):
                self.issue = issue
                self.events = events
                self.event_requests = 0

            def get_repository(self, name):
                return {"id": base, "full_name": name}

            def iter_issues(self, name):
                yield [self.issue]

            def iter_issue_events(self, name, number):
                self.event_requests += 1
                yield self.events

        initial = {"id": base + 1, "number": 1, "created_at": "2024-01-01T00:00:00Z",
                   "updated_at": "2024-01-02T00:00:00Z", "state": "open"}
        changed = {**initial, "updated_at": "2024-02-01T00:00:00Z", "state": "closed"}
        first_event = {"id": base + 2, "event": "labeled", "created_at": "2024-01-02T00:00:00Z"}
        new_event = {"id": base + 3, "event": "closed", "created_at": "2024-02-01T00:00:00Z"}
        try:
            for mode, row, events, expected_requests in (
                ("backfill", initial, [first_event], 1),
                ("sync", initial, [first_event], 0),
                ("sync", changed, [first_event, new_event], 1),
                ("sync", changed, [first_event, new_event], 0),
            ):
                client = Client(row, events)
                batches.append(load_repository(client, store, "test/stg-sync",
                                               datetime(2023, 1, 1, tzinfo=timezone.utc), mode=mode))
                self.assertEqual(client.event_requests, expected_requests)
            with store.connection.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM stg.repositories_raw WHERE repository_id = %s", (base,))
                self.assertEqual(cursor.fetchone()[0], 1)
                cursor.execute("SELECT COUNT(*) FROM stg.issues_raw WHERE issue_id = %s", (base + 1,))
                self.assertEqual(cursor.fetchone()[0], 2)
                cursor.execute("SELECT COUNT(*) FROM stg.issue_events_raw WHERE issue_id = %s", (base + 1,))
                self.assertEqual(cursor.fetchone()[0], 2)
                cursor.execute(
                    "SELECT COUNT(*) FROM stg.issue_observations o JOIN stg.issues_raw i "
                    "ON i.snapshot_id = o.snapshot_id WHERE i.issue_id = %s", (base + 1,)
                )
                self.assertEqual(cursor.fetchone()[0], 4)
                cursor.execute("SELECT COUNT(*), MAX(issue_updated_at) FROM stg.issue_event_loads WHERE issue_id = %s "
                               "AND status = 'succeeded'", (base + 1,))
                self.assertEqual(cursor.fetchone(), (2, datetime(2024, 2, 1, tzinfo=timezone.utc)))
                cursor.execute(
                    "SELECT repository_versions_inserted, issue_versions_inserted, event_versions_inserted "
                    "FROM stg.load_batches WHERE batch_id = %s", (batches[-1],)
                )
                self.assertEqual(cursor.fetchone(), (0, 0, 0))
        finally:
            store.connection.rollback()
            with store.connection:
                with store.connection.cursor() as cursor:
                    cursor.execute("DELETE FROM stg.issue_event_loads WHERE issue_id = %s", (base + 1,))
                    cursor.execute("DELETE FROM stg.event_observations WHERE snapshot_id IN "
                                   "(SELECT snapshot_id FROM stg.issue_events_raw WHERE issue_id = %s)", (base + 1,))
                    cursor.execute("DELETE FROM stg.issue_observations WHERE snapshot_id IN "
                                   "(SELECT snapshot_id FROM stg.issues_raw WHERE issue_id = %s)", (base + 1,))
                    cursor.execute("DELETE FROM stg.repository_observations WHERE snapshot_id IN "
                                   "(SELECT snapshot_id FROM stg.repositories_raw WHERE repository_id = %s)", (base,))
                    cursor.execute("DELETE FROM stg.issue_events_raw WHERE issue_id = %s", (base + 1,))
                    cursor.execute("DELETE FROM stg.issues_raw WHERE issue_id = %s", (base + 1,))
                    cursor.execute("DELETE FROM stg.repositories_raw WHERE repository_id = %s", (base,))
                    cursor.execute("DELETE FROM stg.load_batches WHERE batch_id = ANY(%s::uuid[])", (batches,))
            store.close()

    def test_a_b_a_has_two_snapshots_and_three_observations(self):
        store = StgStore.from_env()
        store.apply_schema()
        issue_id = random.randint(8_000_000_000_000_000, 8_999_999_999_000_000)
        batches = []
        try:
            for title in ("A", "B", "A"):
                batch_id = store.start_batch("test/stg-observations")
                batches.append(batch_id)
                store.save_issues(batch_id, issue_id - 1, [
                    {"id": issue_id, "number": 1, "title": title, "created_at": "2024-01-01T00:00:00Z"}
                ], 1)
                # Replaying the same page in one batch must not add an observation.
                store.save_issues(batch_id, issue_id - 1, [
                    {"id": issue_id, "number": 1, "title": title, "created_at": "2024-01-01T00:00:00Z"}
                ], 1)
                store.finish_batch(batch_id, "succeeded")
            with store.connection.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM stg.issues_raw WHERE issue_id = %s", (issue_id,))
                self.assertEqual(cursor.fetchone()[0], 2)
                cursor.execute(
                    "SELECT b.batch_id, i.raw_json->>'title' "
                    "FROM stg.issue_observations o "
                    "JOIN stg.issues_raw i ON i.snapshot_id = o.snapshot_id "
                    "JOIN stg.load_batches b ON b.batch_id = o.batch_id "
                    "WHERE i.issue_id = %s ORDER BY o.observation_id", (issue_id,)
                )
                rows = cursor.fetchall()
                self.assertEqual([row[1] for row in rows], ["A", "B", "A"])
                self.assertEqual([str(row[0]) for row in rows], batches)
        finally:
            store.connection.rollback()
            with store.connection:
                with store.connection.cursor() as cursor:
                    cursor.execute(
                        "DELETE FROM stg.issue_observations WHERE snapshot_id IN "
                        "(SELECT snapshot_id FROM stg.issues_raw WHERE issue_id = %s)", (issue_id,)
                    )
                    cursor.execute("DELETE FROM stg.issues_raw WHERE issue_id = %s", (issue_id,))
                    cursor.execute("DELETE FROM stg.load_batches WHERE batch_id = ANY(%s::uuid[])", (batches,))
            store.close()

    def test_partial_event_load_then_retry_keeps_events_and_completes(self):
        store = StgStore.from_env()
        store.apply_schema()
        issue_id = random.randint(8_000_000_000_000_000, 8_999_999_999_000_000)
        repository_id = issue_id - 1
        row = {"id": issue_id, "number": 1, "created_at": "2024-01-01T00:00:00Z",
               "updated_at": "2024-01-02T00:00:00Z"}
        event = {"id": issue_id + 1, "event": "closed", "created_at": "2024-01-02T00:00:00Z"}
        batches = []
        try:
            first = store.start_batch("test/stg-partial")
            batches.append(first)
            store.save_issues(first, repository_id, [row], 1)
            store.start_issue_events(first, repository_id, row)
            store.save_events(first, repository_id, issue_id, [event])
            store.finish_issue_events(first, issue_id, "failed", "second page failed")
            store.finish_batch(first, "failed", "second page failed")
            self.assertFalse(store.has_completed_events(issue_id, row["updated_at"], "backfill"))

            second = store.start_batch("test/stg-partial")
            batches.append(second)
            store.save_issues(second, repository_id, [row], 1)
            store.start_issue_events(second, repository_id, row)
            store.save_events(second, repository_id, issue_id, [event])
            store.finish_issue_events(second, issue_id, "succeeded")
            store.finish_batch(second, "succeeded")
            self.assertTrue(store.has_completed_events(issue_id, row["updated_at"], "backfill"))
            with store.connection.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM stg.issue_events_raw WHERE event_id = %s", (issue_id + 1,))
                self.assertEqual(cursor.fetchone()[0], 1)
                cursor.execute(
                    "SELECT status, events_fetched, error_message FROM stg.issue_event_loads "
                    "WHERE issue_id = %s ORDER BY started_at, batch_id", (issue_id,)
                )
                rows = cursor.fetchall()
                self.assertEqual([r[0] for r in rows], ["failed", "succeeded"])
                self.assertEqual([r[1] for r in rows], [1, 1])
                self.assertEqual(rows[0][2], "second page failed")
                cursor.execute(
                    "SELECT event_versions_inserted FROM stg.load_batches WHERE batch_id = %s", (second,)
                )
                self.assertEqual(cursor.fetchone()[0], 0)
        finally:
            store.connection.rollback()
            with store.connection:
                with store.connection.cursor() as cursor:
                    cursor.execute("DELETE FROM stg.issue_event_loads WHERE issue_id = %s", (issue_id,))
                    cursor.execute(
                        "DELETE FROM stg.event_observations WHERE snapshot_id IN "
                        "(SELECT snapshot_id FROM stg.issue_events_raw WHERE event_id = %s)", (issue_id + 1,)
                    )
                    cursor.execute(
                        "DELETE FROM stg.issue_observations WHERE snapshot_id IN "
                        "(SELECT snapshot_id FROM stg.issues_raw WHERE issue_id = %s)", (issue_id,)
                    )
                    cursor.execute("DELETE FROM stg.issue_events_raw WHERE event_id = %s", (issue_id + 1,))
                    cursor.execute("DELETE FROM stg.issues_raw WHERE issue_id = %s", (issue_id,))
                    cursor.execute("DELETE FROM stg.load_batches WHERE batch_id = ANY(%s::uuid[])", (batches,))
            store.close()

    def test_repeat_is_idempotent_and_change_retains_previous_snapshot(self):
        store = StgStore.from_env()
        base_id = random.randint(8_000_000_000_000_000, 8_999_999_999_000_000)
        batch_id = store.start_batch("test/stg-fixture")
        try:
            repository = {"id": base_id, "full_name": "test/stg-fixture", "name": "before"}
            issue = {"id": base_id + 1, "number": 1, "created_at": "2024-01-01T00:00:00Z", "title": "before"}
            event = {"id": base_id + 2, "event": "closed", "created_at": "2024-01-02T00:00:00Z"}
            store.save_repository(batch_id, repository)
            store.save_repository(batch_id, repository)
            store.save_issues(batch_id, base_id, [issue], 1)
            store.save_issues(batch_id, base_id, [issue], 1)
            store.start_issue_events(batch_id, base_id, issue)
            store.save_events(batch_id, base_id, base_id + 1, [event])
            store.save_events(batch_id, base_id, base_id + 1, [event])
            store.finish_issue_events(batch_id, base_id + 1, "succeeded")
            store.save_repository(batch_id, {**repository, "name": "after"})
            store.save_issues(batch_id, base_id, [{**issue, "title": "after"}], 1)
            store.finish_batch(batch_id, "succeeded")
            with store.connection.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM stg.repositories_raw WHERE repository_id = %s", (base_id,))
                self.assertEqual(cursor.fetchone()[0], 2)
                cursor.execute("SELECT COUNT(*) FROM stg.issues_raw WHERE issue_id = %s", (base_id + 1,))
                self.assertEqual(cursor.fetchone()[0], 2)
                cursor.execute("SELECT COUNT(*) FROM stg.issue_events_raw WHERE event_id = %s", (base_id + 2,))
                self.assertEqual(cursor.fetchone()[0], 1)
                cursor.execute(
                    "SELECT repository_versions_inserted, issue_versions_inserted, event_versions_inserted, status "
                    "FROM stg.load_batches WHERE batch_id = %s", (batch_id,)
                )
                self.assertEqual(cursor.fetchone(), (2, 2, 1, "succeeded"))
        finally:
            store.connection.rollback()
            with store.connection:
                with store.connection.cursor() as cursor:
                    cursor.execute("DELETE FROM stg.issue_event_loads WHERE issue_id = %s", (base_id + 1,))
                    cursor.execute(
                        "DELETE FROM stg.event_observations WHERE snapshot_id IN "
                        "(SELECT snapshot_id FROM stg.issue_events_raw WHERE event_id = %s)", (base_id + 2,)
                    )
                    cursor.execute(
                        "DELETE FROM stg.issue_observations WHERE snapshot_id IN "
                        "(SELECT snapshot_id FROM stg.issues_raw WHERE issue_id = %s)", (base_id + 1,)
                    )
                    cursor.execute(
                        "DELETE FROM stg.repository_observations WHERE snapshot_id IN "
                        "(SELECT snapshot_id FROM stg.repositories_raw WHERE repository_id = %s)", (base_id,)
                    )
                    cursor.execute("DELETE FROM stg.issue_events_raw WHERE event_id = %s", (base_id + 2,))
                    cursor.execute("DELETE FROM stg.issues_raw WHERE issue_id = %s", (base_id + 1,))
                    cursor.execute("DELETE FROM stg.repositories_raw WHERE repository_id = %s", (base_id,))
                    cursor.execute("DELETE FROM stg.load_batches WHERE batch_id = %s", (batch_id,))
            store.close()


if __name__ == "__main__":
    unittest.main()

"""Regression tests for per-Issue completeness and restart behavior."""

import unittest
from datetime import datetime, timezone

from src.stg.loader import load_repository


CUTOFF = datetime(2023, 1, 1, tzinfo=timezone.utc)


def issue(issue_id, updated="2024-01-01T00:00:00Z"):
    return {"id": issue_id, "number": issue_id, "created_at": "2023-01-02T00:00:00Z",
            "updated_at": updated}


class MemoryStore:
    def __init__(self):
        self.batches = []
        self.loads = {}
        self.saved_events = set()
        self.issue_snapshots = []

    def start_batch(self, name, mode="backfill", created_since=None):
        batch_id = str(len(self.batches) + 1)
        self.batches.append({"id": batch_id, "status": "running", "mode": mode})
        return batch_id

    def finish_batch(self, batch_id, status, error_message=None):
        self.batches[int(batch_id) - 1]["status"] = status

    def record_issue_scan(self, batch_id, status, passes, issue_count):
        self.batches[int(batch_id) - 1].update(
            scan_status=status, scan_passes=passes, scan_issue_count=issue_count
        )

    def save_repository(self, *args):
        pass

    def save_issues(self, batch_id, repository_id, issues, fetched_count):
        self.issue_snapshots.extend((batch_id, row["id"], row.get("updated_at")) for row in issues)

    def has_completed_events(self, issue_id, updated_at, mode):
        if mode == "audit":
            return False
        successful = [row for row in self.loads.values()
                      if row["issue_id"] == issue_id and row["status"] == "succeeded"]
        return bool(successful) if mode == "backfill" else any(
            row["updated_at"] == updated_at for row in successful
        )

    def start_issue_events(self, batch_id, repository_id, row, mode="backfill"):
        self.loads[(batch_id, row["id"])] = {"issue_id": row["id"], "updated_at": row.get("updated_at"),
                                             "status": "running", "events_fetched": 0, "mode": mode}

    def save_events(self, batch_id, repository_id, issue_id, events):
        self.saved_events.update(event["id"] for event in events)
        self.loads[(batch_id, issue_id)]["events_fetched"] += len(events)

    def finish_issue_events(self, batch_id, issue_id, status, error_message=None):
        row = self.loads[(batch_id, issue_id)]
        row["status"] = status
        row["error_message"] = error_message


class Client:
    def __init__(self, issues, events, fail_issue=None):
        self.issues = issues
        self.events = events
        self.fail_issue = fail_issue
        self.event_requests = []

    def get_repository(self, name):
        return {"id": 10, "full_name": name}

    def iter_issues(self, name):
        yield self.issues

    def iter_issue_events(self, name, number):
        self.event_requests.append(number)
        for page_number, page in enumerate(self.events.get(number, [[]]), 1):
            if self.fail_issue == number and page_number == 2:
                raise RuntimeError("second page failed")
            yield page


class ResumeTests(unittest.TestCase):
    def test_audit_finds_event_without_updated_at_change_and_repeat_is_deduplicated(self):
        store = MemoryStore()
        row = issue(30)
        load_repository(Client([row], {30: [[{"id": 301}]]}), store, "a/b", CUTOFF,
                        max_issues=1, mode="backfill")
        ordinary_sync = Client([row], {30: [[{"id": 301}, {"id": 302}]]})
        load_repository(ordinary_sync, store, "a/b", CUTOFF, max_issues=1, mode="sync")
        self.assertEqual(ordinary_sync.event_requests, [])
        self.assertEqual(store.saved_events, {301})
        for expected_batch in ("3", "4"):
            audit = Client([row], {30: [[{"id": 301}, {"id": 302}]]})
            load_repository(audit, store, "a/b", CUTOFF, max_issues=1, mode="audit")
            self.assertEqual(audit.event_requests, [30])
            self.assertEqual(store.loads[(expected_batch, 30)]["status"], "succeeded")
            self.assertEqual(store.loads[(expected_batch, 30)]["mode"], "audit")
            self.assertEqual(store.batches[int(expected_batch) - 1]["scan_status"], "sample")
        self.assertEqual(store.saved_events, {301, 302})

    def test_control_scan_compares_id_sets_and_loads_missing_issue(self):
        store = MemoryStore()
        one, two, three = issue(1), issue(2), issue(3)

        class MovingClient(Client):
            def __init__(self):
                super().__init__([], {1: [[]], 2: [[]], 3: [[]]})
                self.scans = iter(([one, two], [one, three], [one, three]))
                self.scan_calls = 0

            def iter_issues(self, name):
                self.scan_calls += 1
                yield next(self.scans)

        client = MovingClient()
        load_repository(client, store, "a/b", CUTOFF, mode="backfill")
        self.assertEqual(client.scan_calls, 3)
        self.assertEqual(client.event_requests, [1, 2, 3])
        self.assertEqual({row[1] for row in store.issue_snapshots}, {1, 2, 3})
        self.assertEqual(store.batches[0]["scan_status"], "verified")
        self.assertEqual(store.batches[0]["scan_passes"], 3)

    def test_unstable_control_scans_remain_unverified(self):
        store = MemoryStore()
        rows = [issue(1), issue(2), issue(3)]

        class MovingClient(Client):
            def __init__(self):
                super().__init__([], {1: [[]], 2: [[]], 3: [[]]})
                self.scans = iter((rows[:1], rows[:2], rows[:3]))
                self.scan_calls = 0

            def iter_issues(self, name):
                self.scan_calls += 1
                yield next(self.scans)

        client = MovingClient()
        load_repository(client, store, "a/b", CUTOFF, mode="backfill")
        self.assertEqual(client.scan_calls, 3)
        self.assertEqual(client.event_requests, [1, 2, 3])
        self.assertEqual(store.batches[0]["status"], "succeeded")
        self.assertEqual(store.batches[0]["scan_status"], "unverified")
        self.assertEqual(store.batches[0]["scan_passes"], 3)

    def test_sample_skips_control_scans(self):
        store = MemoryStore()

        class OnePassClient(Client):
            def iter_issues(self, name):
                self.scan_calls = getattr(self, "scan_calls", 0) + 1
                if self.scan_calls > 1:
                    raise AssertionError("sample must not perform a control scan")
                yield [issue(1), issue(2)]

        client = OnePassClient([], {1: [[]], 2: [[]]})
        load_repository(client, store, "a/b", CUTOFF, max_issues=1, mode="audit")
        self.assertEqual(client.event_requests, [1])
        self.assertEqual(store.batches[0]["scan_status"], "sample")
        self.assertEqual(store.batches[0]["scan_passes"], 1)

    def test_running_attempt_does_not_block_safe_retry(self):
        store = MemoryStore()
        row = issue(50)
        store.loads[("interrupted", 50)] = {"issue_id": 50, "updated_at": row["updated_at"],
                                             "status": "running", "events_fetched": 1}
        load_repository(Client([row], {50: [[{"id": 501}]]}), store, "a/b", CUTOFF,
                        max_issues=1, mode="backfill")
        self.assertEqual(store.loads[("interrupted", 50)]["status"], "running")
        self.assertEqual(store.loads[("1", 50)]["status"], "succeeded")

    def test_audit_processes_duplicate_issue_only_once_in_one_batch(self):
        store = MemoryStore()
        row = issue(60)

        class DuplicateClient(Client):
            def iter_issues(self, name):
                yield [row]
                yield [row]

        client = DuplicateClient([], {60: [[{"id": 601}]]})
        load_repository(client, store, "a/b", CUTOFF, max_issues=2, mode="audit")
        self.assertEqual(client.event_requests, [60])
        self.assertEqual(len(store.loads), 1)

    def test_second_event_page_failure_is_retryable_and_keeps_first_page(self):
        store = MemoryStore()
        row = issue(20)
        pages = {20: [[{"id": 1}], [{"id": 2}]]}
        with self.assertRaises(RuntimeError):
            load_repository(Client([row], pages, fail_issue=20), store, "a/b", CUTOFF)
        self.assertEqual(store.saved_events, {1})
        self.assertEqual(store.loads[("1", 20)]["events_fetched"], 1)
        self.assertEqual(store.loads[("1", 20)]["status"], "failed")
        self.assertFalse(store.has_completed_events(20, row["updated_at"], "backfill"))

        load_repository(Client([row], pages), store, "a/b", CUTOFF)
        self.assertEqual(store.saved_events, {1, 2})
        self.assertEqual(store.loads[("2", 20)]["status"], "succeeded")
        self.assertEqual(store.loads[("2", 20)]["events_fetched"], 2)

    def test_backfill_skips_completed_histories_after_restart(self):
        store = MemoryStore()
        rows = [issue(1), issue(2), issue(3)]
        events = {1: [[{"id": 101}]], 2: [[{"id": 201}], [{"id": 202}]], 3: [[{"id": 301}]]}
        with self.assertRaises(RuntimeError):
            load_repository(Client(rows, events, fail_issue=2), store, "a/b", CUTOFF, mode="backfill")
        retry = Client(rows, events)
        load_repository(retry, store, "a/b", CUTOFF, mode="backfill")
        self.assertEqual(retry.event_requests, [2, 3])
        self.assertEqual(store.saved_events, {101, 201, 202, 301})
        self.assertEqual(store.batches[-1]["status"], "succeeded")

    def test_sync_fetches_changed_issue_and_skips_unchanged(self):
        store = MemoryStore()
        old = issue(4)
        first = Client([old], {4: [[{"id": 401}]]})
        load_repository(first, store, "a/b", CUTOFF, mode="backfill")
        unchanged = Client([old], {4: [[{"id": 401}]]})
        load_repository(unchanged, store, "a/b", CUTOFF, mode="sync")
        self.assertEqual(unchanged.event_requests, [])
        changed = issue(4, "2024-02-01T00:00:00Z")
        updated = Client([changed], {4: [[{"id": 401}, {"id": 402}]]})
        load_repository(updated, store, "a/b", CUTOFF, mode="sync")
        self.assertEqual(updated.event_requests, [4])
        self.assertEqual(store.saved_events, {401, 402})
        self.assertEqual(store.loads[("3", 4)]["status"], "succeeded")
        self.assertEqual(store.loads[("3", 4)]["updated_at"], changed["updated_at"])


if __name__ == "__main__":
    unittest.main()

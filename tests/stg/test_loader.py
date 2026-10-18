import unittest
from datetime import datetime, timezone

from src.stg.loader import select_issues, load_repository


class LoaderTests(unittest.TestCase):
    def test_excludes_pull_requests_and_old_issues_inclusive_boundary(self):
        cutoff = datetime(2023, 1, 1, tzinfo=timezone.utc)
        page = [
            {"id": 1, "number": 11, "created_at": "2023-01-02T00:00:00Z"},
            {"id": 2, "number": 12, "created_at": "2023-01-01T00:00:00Z", "pull_request": {}},
            {"id": 3, "number": 13, "created_at": "2023-01-01T00:00:00Z"},
            {"id": 4, "number": 14, "created_at": "2022-12-31T23:59:59Z"},
        ]
        selected, reached_cutoff = select_issues(page, cutoff)
        self.assertEqual([issue["id"] for issue in selected], [1, 3])
        self.assertTrue(reached_cutoff)

    def test_failed_event_request_marks_batch_failed(self):
        class Client:
            def get_repository(self, name):
                return {"id": 10, "full_name": name}

            def iter_issues(self, name):
                yield [{"id": 20, "number": 4, "created_at": "2024-01-01T00:00:00Z"}]

            def iter_issue_events(self, name, number):
                raise RuntimeError("API failed")
                yield []

        class Store:
            status = None

            def start_batch(self, name, mode="backfill"):
                return "batch"

            def save_repository(self, *args):
                pass

            def save_issues(self, *args):
                pass

            def has_completed_events(self, *args):
                return False

            def start_issue_events(self, *args, **kwargs):
                pass

            def finish_issue_events(self, *args, **kwargs):
                pass

            def finish_batch(self, batch_id, status, error_message=None):
                self.status = status

            def record_issue_scan(self, *args):
                pass

        store = Store()
        with self.assertRaises(RuntimeError):
            load_repository(Client(), store, "a/b", datetime(2023, 1, 1, tzinfo=timezone.utc), max_issues=1)
        self.assertEqual(store.status, "failed")


if __name__ == "__main__":
    unittest.main()

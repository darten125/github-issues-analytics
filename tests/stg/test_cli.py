import os
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from src.stg.cli import main


class CliTests(unittest.TestCase):
    def test_audit_mode_is_forwarded_to_loader(self):
        with patch.dict(os.environ, {"GITHUB_TOKEN": "dummy"}), \
             patch("src.stg.cli.StgStore.from_env") as store_factory, \
             patch("src.stg.cli.GitHubClient"), \
             patch("src.stg.cli.load_repository") as load:
            self.assertEqual(main(["load", "--repository", "Kotlin/kotlinx.coroutines",
                                   "--mode", "audit", "--max-issues", "2"]), 0)
            self.assertEqual(load.call_args.kwargs["mode"], "audit")
            store_factory.return_value.close.assert_called_once()

    def test_sync_mode_is_forwarded_to_loader(self):
        with patch.dict(os.environ, {"GITHUB_TOKEN": "dummy"}), \
             patch("src.stg.cli.StgStore.from_env") as store_factory, \
             patch("src.stg.cli.GitHubClient"), \
             patch("src.stg.cli.load_repository") as load:
            self.assertEqual(main(["load", "--repository", "Kotlin/kotlinx.coroutines",
                                   "--mode", "sync"]), 0)
            self.assertEqual(load.call_args.kwargs["mode"], "sync")
            store_factory.return_value.close.assert_called_once()

    def test_missing_token_fails_before_loading(self):
        with patch.dict(os.environ, {"GITHUB_TOKEN": ""}):
            self.assertEqual(main(["load", "--repository", "Kotlin/kotlinx.coroutines", "--max-issues", "1"]), 1)

    def test_invalid_sample_size_is_rejected(self):
        with patch.dict(os.environ, {"GITHUB_TOKEN": "dummy"}):
            self.assertEqual(main(["load", "--max-issues", "0"]), 1)


    def test_attest_cohort_parses_boundary_without_github_token(self):
        batch_id = "00000000-0000-0000-0000-00000000000a"
        with patch.dict(os.environ, {"GITHUB_TOKEN": ""}), \
             patch("src.stg.cli.StgStore.from_env") as store_factory:
            store = store_factory.return_value
            store.attest_created_since.return_value = "attested"
            self.assertEqual(main(["attest-cohort", "--batch-id", batch_id,
                                   "--created-since", "2023-01-01T00:00:00Z",
                                   "--evidence", "DagRun conf"]), 0)
            store.attest_created_since.assert_called_once_with(
                batch_id, datetime(2023, 1, 1, tzinfo=timezone.utc), "DagRun conf")
            store.attest_created_since.side_effect = ValueError("batch is still running")
            with self.assertLogs(level="ERROR") as logs:
                self.assertEqual(main(["attest-cohort", "--batch-id", batch_id,
                                       "--created-since", "2023-01-01T00:00:00Z",
                                       "--evidence", "DagRun conf"]), 1)
            self.assertIn("still running", "\n".join(logs.output))
        self.assertEqual(main(["attest-cohort", "--batch-id", batch_id,
                               "--created-since", "2023-01-01", "--evidence", "x"]), 1)

if __name__ == "__main__":
    unittest.main()

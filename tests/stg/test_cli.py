import os
import unittest
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


if __name__ == "__main__":
    unittest.main()

"""Unit tests for the small Airflow-to-STG adapter (no Airflow scheduler needed)."""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from src.stg.github_client import GitHubApiError


BASE = {
    "repository": "Kotlin/kotlinx.coroutines",
    "mode": "backfill",
    "created_since": "2023-01-01T00:00:00Z",
    "max_issues": 8,
    "full_load": False,
}


class AdapterTests(unittest.TestCase):
    def test_rejects_unsafe_or_invalid_run_parameters(self):
        from src.stg.airflow_adapter import normalize_run_params

        invalid = (
            {"max_issues": None},
            {"max_issues": -1},
            {"max_issues": 0},
            {"max_issues": True},
            {"full_load": True},
            {"full_load": "true"},
            {"repository": "someone/else"},
            {"mode": "other"},
            {"created_since": "not-a-date"},
            {"created_since": "2023-01-01"},
        )
        for overrides in invalid:
            with self.subTest(overrides=overrides), self.assertRaises(ValueError):
                normalize_run_params(BASE | overrides)

    def test_full_load_requires_explicit_flag_and_null_limit(self):
        from src.stg.airflow_adapter import normalize_run_params

        result = normalize_run_params(BASE | {
            "repository": "all", "mode": "audit", "full_load": True, "max_issues": None,
        })
        self.assertEqual(result["repositories"], [
            "Kotlin/kotlinx.coroutines", "ktorio/ktor", "android/nowinandroid",
        ])
        self.assertIsNone(result["max_issues"])
        self.assertEqual(result["mode"], "audit")

    def test_unselected_repository_does_not_contact_github_or_postgres(self):
        from src.stg.airflow_adapter import load_selected_repository

        with patch("src.stg.airflow_adapter.StgStore.from_env") as connect:
            result = load_selected_repository(BASE | {"repositories": [BASE["repository"]]}, "ktorio/ktor")
        self.assertIsNone(result)
        connect.assert_not_called()

    def test_github_failure_propagates_and_closes_resources(self):
        from src.stg.airflow_adapter import load_selected_repository

        store = MagicMock()
        client = MagicMock()
        with patch("src.stg.airflow_adapter.StgStore.from_env", return_value=store), \
             patch("src.stg.airflow_adapter.GitHubClient", return_value=client), \
             patch("src.stg.airflow_adapter.load_repository", side_effect=GitHubApiError("controlled API error")), \
             patch.dict("os.environ", {"GITHUB_TOKEN": "unit-test-token"}):
            with self.assertRaisesRegex(GitHubApiError, "controlled API error"):
                load_selected_repository(BASE | {"repositories": [BASE["repository"]]}, BASE["repository"])
        store.close.assert_called_once()
        client.session.close.assert_called_once()

    def test_unverified_full_batch_fails_verification_without_rewriting_it(self):
        from src.stg.airflow_adapter import verify_batch

        cursor = MagicMock()
        cursor.fetchone.return_value = (
            "batch-1", "Kotlin/kotlinx.coroutines", "succeeded", "unverified", 3, 8, 8, 30, 0, 0, 0,
        )
        store = MagicMock()
        store.connection.cursor.return_value.__enter__.return_value = cursor
        settings = BASE | {"full_load": True, "max_issues": None, "repositories": [BASE["repository"]]}
        with patch("src.stg.airflow_adapter.StgStore.from_env", return_value=store):
            with self.assertRaisesRegex(RuntimeError, "unverified"):
                verify_batch(settings, {"repository": BASE["repository"], "batch_id": "batch-1"})
        store.close.assert_called_once()
        self.assertEqual(cursor.execute.call_count, 1)
        self.assertIn("SELECT", cursor.execute.call_args.args[0])

    def test_sample_batch_is_verified_and_returns_only_small_metadata(self):
        from src.stg.airflow_adapter import verify_batch

        cursor = MagicMock()
        cursor.fetchone.return_value = (
            "batch-2", "Kotlin/kotlinx.coroutines", "succeeded", "sample", 1, 8, 8, 30, 0, 0, 0,
        )
        store = MagicMock()
        store.connection.cursor.return_value.__enter__.return_value = cursor
        with patch("src.stg.airflow_adapter.StgStore.from_env", return_value=store):
            result = verify_batch(BASE | {"repositories": [BASE["repository"]]},
                                  {"repository": BASE["repository"], "batch_id": "batch-2"})
        self.assertEqual(result["batch_id"], "batch-2")
        self.assertEqual(result["issue_scan_status"], "sample")
        self.assertNotIn("raw_json", result)

    def test_failed_or_running_batch_never_passes_verification(self):
        from src.stg.airflow_adapter import verify_batch

        for status in ("failed", "running"):
            with self.subTest(status=status):
                cursor = MagicMock()
                cursor.fetchone.return_value = (
                    "batch-3", BASE["repository"], status, "sample", 1, 8, 8, 0, 0, 0, 0,
                )
                store = MagicMock()
                store.connection.cursor.return_value.__enter__.return_value = cursor
                with patch("src.stg.airflow_adapter.StgStore.from_env", return_value=store):
                    with self.assertRaisesRegex(RuntimeError, status):
                        verify_batch(BASE | {"repositories": [BASE["repository"]]},
                                     {"repository": BASE["repository"], "batch_id": "batch-3"})

    def test_verified_full_batch_passes_and_summary_requires_selected_order(self):
        from src.stg.airflow_adapter import build_summary, verify_batch

        cursor = MagicMock()
        cursor.fetchone.return_value = (
            "batch-4", BASE["repository"], "succeeded", "verified", 2, 8, 8, 30, 0, 0, 0,
        )
        store = MagicMock()
        store.connection.cursor.return_value.__enter__.return_value = cursor
        settings = BASE | {"full_load": True, "max_issues": None, "repositories": [BASE["repository"]]}
        with patch("src.stg.airflow_adapter.StgStore.from_env", return_value=store):
            checked = verify_batch(settings, {"repository": BASE["repository"], "batch_id": "batch-4"})
        summary = build_summary(settings, checked, None, None)
        self.assertEqual(summary["batches"][0]["issue_scan_status"], "verified")
        with self.assertRaisesRegex(RuntimeError, "does not match"):
            build_summary(settings, None, None, None)


if __name__ == "__main__":
    unittest.main()

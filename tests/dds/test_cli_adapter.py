"""CLI and Airflow boundary contracts, with no driver or database required."""

from contextlib import redirect_stderr, redirect_stdout
import copy
from datetime import datetime, timezone
from io import StringIO
import json
import unittest
from unittest.mock import patch
from uuid import UUID

from src.dds import DdsError
from src.dds import airflow_adapter, cli
from src.stg.config import REPOSITORIES

COHORT = datetime(2023, 1, 1, tzinfo=timezone.utc)


BATCH_IDS = ["a0000000-0000-0000-0000-000000000001",
             "b0000000-0000-0000-0000-000000000002",
             "c0000000-0000-0000-0000-000000000003"]


def summary(count=1):
    return {"status": "succeeded", "full_load": True, "mode": "audit",
            "batches": [{"repository": name, "batch_id": batch_id,
                         "status": "succeeded", "issue_scan_status": "verified",
                         "created_since": "2023-01-01T00:00:00Z"}
                        for name, batch_id in zip(REPOSITORIES[:count], BATCH_IDS[:count])]}


class CliTests(unittest.TestCase):
    def invoke(self, args):
        output = StringIO()
        with redirect_stdout(output):
            code = cli.main(args)
        return code, output.getvalue()

    def test_init_db_routes_and_outputs_only_schema_metadata(self):
        with patch.object(cli, "initialize_dds") as initialize:
            code, output = self.invoke(["init-db"])
        self.assertEqual(0, code)
        initialize.assert_called_once_with()
        self.assertEqual({"status": "schema_ready"}, json.loads(output))
        self.assertEqual(1, len(output.splitlines()))

    def test_load_and_preview_pass_explicit_batch_ids(self):
        for command, function in (("load", "load_dds"), ("preview", "preview_dds")):
            with self.subTest(command=command), patch.object(cli, function) as operation:
                expected = {"status": command, "issues": 2, "warnings": 0}
                operation.return_value = expected
                code, output = self.invoke([command, "--batch-id", BATCH_IDS[0],
                                            "--batch-id", BATCH_IDS[1]])
                operation.assert_called_once_with(BATCH_IDS[:2])
                self.assertEqual(0, code)
                self.assertEqual(expected, json.loads(output))
                self.assertEqual(1, len(output.splitlines()))

    def test_validate_without_expected_batches(self):
        with patch.object(cli, "validate_dds", return_value={"status": "valid"}) as validate:
            code, output = self.invoke(["validate"])
        validate.assert_called_once_with(expected_batch_ids=None)
        self.assertEqual(0, code)
        self.assertEqual({"status": "valid"}, json.loads(output))

    def test_validate_with_expected_batches(self):
        with patch.object(cli, "validate_dds", return_value={"status": "valid"}) as validate:
            code, _ = self.invoke(["validate", "--batch-id", BATCH_IDS[0],
                                   "--batch-id", BATCH_IDS[1]])
        validate.assert_called_once_with(expected_batch_ids=BATCH_IDS[:2])
        self.assertEqual(0, code)

    def test_quality_error_returns_one_without_raw_payload_details(self):
        secret = "PRIVATE_RAW_PAYLOAD_AND_TOKEN"
        error = DdsError("source_state_mismatch", issue_id=22, details={"raw_json": secret})
        with patch.object(cli, "load_dds", side_effect=error), self.assertLogs(level="ERROR") as logs:
            code, output = self.invoke(["load", "--batch-id", BATCH_IDS[0]])
        self.assertEqual(1, code)
        self.assertEqual("", output)
        rendered = "\n".join(logs.output)
        self.assertIn("source_state_mismatch", rendered)
        self.assertNotIn(secret, rendered)
        self.assertNotIn("raw_json", rendered)

    def test_driver_error_returns_one_without_connection_secrets(self):
        secret = "postgresql://private_user:PRIVATE_PASSWORD@database/private_db"
        with patch.object(cli, "initialize_dds", side_effect=RuntimeError(secret)):
            with self.assertLogs(level="ERROR") as logs:
                code, output = self.invoke(["init-db"])
        self.assertEqual(1, code)
        self.assertEqual("", output)
        self.assertIn("RuntimeError", "\n".join(logs.output))
        self.assertNotIn(secret, "\n".join(logs.output))
        self.assertNotIn("PRIVATE_PASSWORD", "\n".join(logs.output))

    def test_cli_requires_batches_for_load_and_preview(self):
        for command in ("load", "preview"):
            with self.subTest(command=command), redirect_stderr(StringIO()):
                with self.assertRaises(SystemExit) as caught:
                    cli.main([command])
                self.assertEqual(2, caught.exception.code)


class AdapterTests(unittest.TestCase):
    def test_one_and_three_repository_summaries_pass_exact_name_mapping(self):
        for count in (1, 3):
            with self.subTest(count=count), patch.object(airflow_adapter, "load_dds") as load:
                expected_result = {"status": "succeeded", "issues": 22}
                load.return_value = expected_result
                data = summary(count)
                original = copy.deepcopy(data)
                outcome = airflow_adapter.run_from_stg_summary(data)
                self.assertIs(expected_result, outcome)
                load.assert_called_once_with(
                    BATCH_IDS[:count],
                    expected_repositories=dict(zip(BATCH_IDS[:count], REPOSITORIES[:count])),
                    expected_created_since={batch_id: COHORT for batch_id in BATCH_IDS[:count]},
                )
                self.assertEqual(original, data)

    def test_uuid_spelling_is_canonicalized_before_core_call(self):
        data = summary()
        data["batches"][0]["batch_id"] = "{" + BATCH_IDS[0].upper() + "}"
        with patch.object(airflow_adapter, "load_dds") as load:
            airflow_adapter.run_from_stg_summary(data)
        canonical = str(UUID(BATCH_IDS[0]))
        load.assert_called_once_with([canonical], expected_repositories={canonical: REPOSITORIES[0]},
                                     expected_created_since={canonical: COHORT})

    def test_invalid_summary_shape_and_failed_status_do_not_call_core(self):
        invalid = [None, [], {}, {"status": "failed", "full_load": True, "batches": []},
                   {**summary(), "batches": None}, {**summary(), "batches": []},
                   {**summary(), "batches": summary(3)["batches"] + summary()["batches"]},
                   {**summary(), "batches": [None]},
                   {**summary(), "batches": [{"repository": "unknown/repo", "batch_id": BATCH_IDS[0]}]},
                   {**summary(), "batches": [{"repository": REPOSITORIES[0], "batch_id": BATCH_IDS[0]}]},
                   {**summary(), "batches": [{"repository": REPOSITORIES[0], "batch_id": BATCH_IDS[0],
                                              "created_since": "2023-01-01"}]}]
        for data in invalid:
            with self.subTest(data=data), patch.object(airflow_adapter, "load_dds") as load:
                with self.assertRaises(DdsError):
                    airflow_adapter.run_from_stg_summary(data)
                load.assert_not_called()

    def test_sample_summary_requires_full_load_true(self):
        for full_load in (False, None, 1, "true"):
            data = summary()
            data["full_load"] = full_load
            data["batches"][0]["issue_scan_status"] = "sample"
            with self.subTest(full_load=full_load), patch.object(airflow_adapter, "load_dds") as load:
                with self.assertRaises(DdsError):
                    airflow_adapter.run_from_stg_summary(data)
                load.assert_not_called()

    def test_duplicate_repositories_are_rejected(self):
        data = summary(2)
        data["batches"][1]["repository"] = REPOSITORIES[0]
        with patch.object(airflow_adapter, "load_dds") as load:
            with self.assertRaises(DdsError) as caught:
                airflow_adapter.run_from_stg_summary(data)
        self.assertEqual("duplicate_repository", caught.exception.problem["code"])
        load.assert_not_called()

    def test_invalid_or_missing_uuid_is_rejected(self):
        for batch_id in (None, "", "invalid"):
            data = summary()
            data["batches"][0]["batch_id"] = batch_id
            with self.subTest(batch_id=batch_id), patch.object(airflow_adapter, "load_dds") as load:
                with self.assertRaises(DdsError) as caught:
                    airflow_adapter.run_from_stg_summary(data)
                self.assertEqual("invalid_batch_id", caught.exception.problem["code"])
                load.assert_not_called()

    def test_duplicate_uuid_cannot_collapse_different_repository_claims(self):
        data = summary(2)
        data["batches"][1]["batch_id"] = "{" + BATCH_IDS[0].upper() + "}"
        with patch.object(airflow_adapter, "load_dds") as load:
            with self.assertRaises(DdsError) as caught:
                airflow_adapter.run_from_stg_summary(data)
        self.assertEqual("duplicate_batch_id", caught.exception.problem["code"])
        load.assert_not_called()

    def test_core_rechecks_statuses_instead_of_trusting_summary_counters(self):
        data = summary()
        data["batches"][0].update(status="succeeded", issue_scan_status="verified", issues_selected=999)
        error = DdsError("batch_not_verified")
        with patch.object(airflow_adapter, "load_dds", side_effect=error) as load:
            with self.assertRaises(DdsError) as caught:
                airflow_adapter.run_from_stg_summary(data)
        self.assertIs(error, caught.exception)
        load.assert_called_once_with([BATCH_IDS[0]], expected_repositories={BATCH_IDS[0]: REPOSITORIES[0]},
                                     expected_created_since={BATCH_IDS[0]: COHORT})

    def test_processing_errors_propagate_unchanged(self):
        error = RuntimeError("operator sees task failure")
        with patch.object(airflow_adapter, "load_dds", side_effect=error):
            with self.assertRaises(RuntimeError) as caught:
                airflow_adapter.run_from_stg_summary(summary())
        self.assertIs(error, caught.exception)


if __name__ == "__main__":
    unittest.main()

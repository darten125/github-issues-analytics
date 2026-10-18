"""Test-database guard; needs the driver for DSN parsing but never a server."""

import importlib.util
import os
import unittest
from unittest.mock import patch

RUNTIME = {"DWH_DB_HOST": "analytics-db", "DWH_DB_PORT": "5432", "DWH_DB_NAME": "github_issues_test"}


@unittest.skipUnless(importlib.util.find_spec("psycopg2"), "psycopg2 parses DSNs")
class GuardTests(unittest.TestCase):
    def test_name_must_end_with_test_and_dsn_is_required(self):
        from src.dds.testdb import require_test_database
        for dsn in (None, "", "dbname=github_issues", "postgresql://u:p@h:5432/github_issues"):
            with self.subTest(dsn=dsn), self.assertRaises(RuntimeError):
                require_test_database(dsn, {})

    def test_same_address_is_rejected_without_runtime_password_or_user(self):
        from src.dds.testdb import require_test_database
        for dsn in ("postgresql://u:p@analytics-db:5432/github_issues_test",
                    "host=analytics-db dbname=github_issues_test user=other"):
            with self.subTest(dsn=dsn), self.assertRaises(RuntimeError):
                require_test_database(dsn, RUNTIME)

    def test_local_aliases_and_default_port_are_one_address(self):
        from src.dds.testdb import require_test_database
        runtime = {"DWH_DB_HOST": "127.0.0.1", "DWH_DB_NAME": "github_issues_test"}
        with self.assertRaises(RuntimeError):
            require_test_database("postgresql://u:p@localhost/github_issues_test", runtime)
        with self.assertRaises(RuntimeError):
            require_test_database("dbname=github_issues_test", {"DWH_DB_NAME": "github_issues_test"})

    def test_different_database_server_or_port_is_allowed(self):
        from src.dds.testdb import require_test_database
        for dsn in ("postgresql://u:p@analytics-db:5432/dds_test",
                    "postgresql://u:p@other-host:5432/github_issues_test",
                    "postgresql://u:p@analytics-db:5433/github_issues_test"):
            with self.subTest(dsn=dsn):
                self.assertTrue(require_test_database(dsn, RUNTIME)["dbname"].endswith("_test"))

    def test_guard_runs_before_any_connection(self):
        import psycopg2
        import test_integration
        from src.dds import fixture

        same = "postgresql://u:p@analytics-db:5432/github_issues_test"
        with patch.dict(os.environ, RUNTIME | {"DDS_TEST_DSN": same}, clear=False), \
                patch.object(psycopg2, "connect") as connect:
            os.environ.pop("DWH_DB_PASSWORD", None)
            with self.assertRaises(RuntimeError):
                test_integration.DdsIntegrationTests.setUpClass()
            with self.assertLogs(level="ERROR"):
                self.assertEqual(1, fixture.main())
        connect.assert_not_called()


if __name__ == "__main__":
    unittest.main()

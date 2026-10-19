"""CDM boundary and nine-Issue metric reconciliation on an isolated database."""

from __future__ import annotations

import os
import unittest
from datetime import date, datetime, timezone
from unittest.mock import patch

from src.cdm import CdmError
from src.cdm.loader import _days, _duration_days


class CdmDayRulesTests(unittest.TestCase):
    def test_airflow_rejects_incomplete_dds_result(self):
        from src.cdm.airflow_adapter import run_after_dds

        with self.assertRaises(CdmError):
            run_after_dds({"status": "succeeded", "coverage_complete": True,
                           "ready_for_cdm": True,
                           "repositories": [{"repository": "ktorio/ktor"}]})

    def test_midnight_transition_belongs_to_next_day(self):
        start = datetime(2023, 3, 31, tzinfo=timezone.utc)
        midnight = datetime(2023, 4, 1, tzinfo=timezone.utc)
        issue = {"issue_id": 1, "repository_id": 2, "created_at": start}
        versions = [
            {"valid_from": start, "valid_to": midnight, "status": "open"},
            {"valid_from": midnight, "valid_to": None, "status": "closed"},
        ]
        rows = list(_days([(issue, versions, midnight, None,
                            _duration_days(start, midnight))], date(2023, 4, 1)))
        self.assertEqual([(date(2023, 3, 31), "open", False),
                          (date(2023, 4, 1), "closed", True)],
                         [(row[1], row[3], row[5]) for row in rows])


@unittest.skipUnless(os.getenv("CDM_INTEGRATION") == "1", "set CDM_INTEGRATION=1 explicitly")
class CdmIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg2

        from src.dds.fixture import build_stg
        from src.dds.testdb import require_test_database
        from src.stg.store import StgStore

        cls.dsn = os.getenv("DDS_TEST_DSN") or ""
        settings = require_test_database(cls.dsn)
        cls.old_env = {f"DWH_DB_{key}": os.getenv(f"DWH_DB_{key}")
                       for key in ("HOST", "PORT", "NAME", "USER", "PASSWORD")}
        for key, setting in (("HOST", "host"), ("PORT", "port"), ("NAME", "dbname"),
                             ("USER", "user"), ("PASSWORD", "password")):
            os.environ[f"DWH_DB_{key}"] = str(settings.get(setting) or
                                             ("5432" if key == "PORT" else ""))
        connection = psycopg2.connect(cls.dsn)
        try:
            if not 160000 <= connection.server_version < 170000:
                raise RuntimeError("CDM integration tests require PostgreSQL 16")
            store = StgStore(connection)
            store.apply_schema()
            cls.batch_ids = build_stg(store)
        finally:
            connection.close()
        from src.dds.loader import load_dds
        load_dds(cls.batch_ids)

    @classmethod
    def tearDownClass(cls):
        for key, previous in cls.old_env.items():
            if previous is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = previous

    def scalar(self, query, params=()):
        import psycopg2
        with psycopg2.connect(self.dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchone()[0]

    def test_reference_metrics_idempotence_and_history_change(self):
        from src.cdm.loader import load_cdm, validate_cdm
        from src.dds.fixture import ISSUE_IDS

        with patch.dict(os.environ, {"CDM_READ_PASSWORD": ""}):
            first = load_cdm()
            self.assertEqual(first["status"], "succeeded")
            self.assertEqual(first["issues"], 9)
            self.assertEqual(first["labels"], 8)
            self.assertEqual(validate_cdm()["status"], "succeeded")
            self.assertEqual(load_cdm()["status"], "unchanged")
            with self.assertRaises(CdmError):
                load_cdm(expected_repositories=[{
                    "repository": "Kotlin/kotlinx.coroutines",
                    "batch_id": "00000000-0000-0000-0000-000000000000",
                    "run_id": "00000000-0000-0000-0000-000000000000",
                }])
            self.assertEqual("dds_publication_changed", self.scalar(
                "SELECT error_code FROM cdm.load_runs WHERE status='failed' "
                "ORDER BY started_at DESC LIMIT 1"))
            self.assertEqual(first["run_id"], str(self.scalar(
                "SELECT run_id FROM cdm.current_state WHERE singleton")))

            day = date(2023, 3, 31)
            self.assertEqual(4, self.scalar(
                "SELECT count(*) FROM cdm.issue_day WHERE report_day BETWEEN "
                "DATE '2023-03-01' AND %s AND is_new", (day,)))
            self.assertEqual((8, 2, 6), self._counts(day))
            self.assertEqual(4, self.scalar(
                "SELECT count(*) FROM cdm.issue_day WHERE report_day=%s AND ever_closed", (day,)))
            self.assertEqual(2, self.scalar(
                "SELECT count(*) FROM cdm.issue_day WHERE report_day=%s AND ever_reopened", (day,)))
            self.assertEqual(10, round(self.scalar("""
                SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY first_close_days)
                FROM cdm.issue_day WHERE report_day=%s AND ever_closed
            """, (day,))))
            self.assertEqual(20, round(self.scalar("""
                SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY first_close_days)
                FROM cdm.issue_lifecycle
                WHERE first_closed_at >= TIMESTAMPTZ '2023-03-01 00:00+00'
                  AND first_closed_at < TIMESTAMPTZ '2023-04-01 00:00+00'
            """)))
            self.assertEqual(6, self.scalar(
                "SELECT count(DISTINCT issue_id) FROM cdm.current_issue_label"))
            self.assertEqual(8, self.scalar(
                "SELECT count(*) FROM cdm.current_issue_label"))
            self.assertEqual(7, self.scalar("""
                SELECT count(*) FROM cdm.issue_day d
                JOIN cdm.issue_lifecycle i USING (issue_id)
                WHERE d.report_day=DATE '2023-03-31' AND i.author_id IS NOT NULL
            """))
            self.assertEqual([(2, 0, 2), (4, 2, 2), (2, 0, 2)],
                             self._per_repo_status(day))
            self.assertIsNone(self.scalar("""
                SELECT count(*) FILTER (WHERE ever_reopened)::numeric
                     / nullif(count(*) FILTER (WHERE ever_closed), 0)
                FROM cdm.issue_day d JOIN cdm.repository_snapshot r USING (repository_id)
                WHERE d.report_day=DATE '2023-03-31' AND r.full_name='ktorio/ktor'
            """))
            # Closing exactly at midnight does not count for March 31.
            self.assertEqual("open", self.scalar(
                "SELECT status_eod FROM cdm.issue_day WHERE issue_id=%s AND report_day=%s",
                (ISSUE_IDS[-1], day)))

            # A corrected transition changes historical output even with the
            # same source batch IDs.
            import psycopg2
            issue_id = ISSUE_IDS[0]
            previous_run = self.scalar(
                "SELECT run_id FROM cdm.current_state WHERE singleton")
            previous_days = self.scalar("SELECT count(*) FROM cdm.issue_day")
            with psycopg2.connect(self.dsn) as connection:
                with connection.cursor() as cursor:
                    cursor.execute("""
                        UPDATE dds.issue_events
                        SET github_created_at = TIMESTAMPTZ '2023-01-16 10:00+00'
                        WHERE issue_id=%s AND status_after='closed'
                    """, (issue_id,))
                    cursor.execute("""
                        UPDATE dds.issue_status_versions
                        SET valid_to = TIMESTAMPTZ '2023-01-16 10:00+00'
                        WHERE issue_id=%s AND status='open'
                    """, (issue_id,))
                    cursor.execute("""
                        UPDATE dds.issue_status_versions
                        SET valid_from = TIMESTAMPTZ '2023-01-16 10:00+00'
                        WHERE issue_id=%s AND status='closed'
                    """, (issue_id,))
            with self.assertRaises(CdmError):
                validate_cdm()
            # Force a failure after the replacement transaction has deleted
            # the old business rows. Its rollback must preserve publication.
            with patch("src.cdm.loader._days", side_effect=CdmError("forced_test_failure")):
                with self.assertRaises(CdmError):
                    load_cdm()
            self.assertEqual(previous_run, self.scalar(
                "SELECT run_id FROM cdm.current_state WHERE singleton"))
            self.assertEqual(previous_days, self.scalar(
                "SELECT count(*) FROM cdm.issue_day"))
            self.assertEqual("succeeded", load_cdm()["status"])
            self.assertEqual(11, self.scalar(
                "SELECT first_close_days FROM cdm.issue_lifecycle WHERE issue_id=%s", (issue_id,)))

    def _counts(self, day):
        import psycopg2
        with psycopg2.connect(self.dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT count(*), count(*) FILTER (WHERE status_eod='closed'),
                           count(*) FILTER (WHERE status_eod='open')
                    FROM cdm.issue_day WHERE report_day=%s
                """, (day,))
                return cursor.fetchone()

    def _per_repo_status(self, day):
        import psycopg2
        with psycopg2.connect(self.dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT count(*), count(*) FILTER (WHERE status_eod='closed'),
                           count(*) FILTER (WHERE status_eod='open')
                    FROM cdm.issue_day d JOIN cdm.repository_snapshot r USING (repository_id)
                    WHERE d.report_day=%s
                    GROUP BY r.full_name ORDER BY r.full_name
                """, (day,))
                return cursor.fetchall()

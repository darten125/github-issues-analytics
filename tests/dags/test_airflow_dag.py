"""Check the parsed Airflow 3 DAG and its serial repository chain."""

from __future__ import annotations

import unittest

from airflow.dag_processing.dagbag import DagBag


class DagDefinitionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bag = DagBag("/opt/airflow/dags")
        cls.dag = cls.bag.get_dag("github_issues_stg")

    def test_import_schedule_and_concurrency(self):
        self.assertEqual(self.bag.import_errors, {})
        self.assertIsNotNone(self.dag)
        self.assertIsNone(self.dag.schedule)
        self.assertFalse(self.dag.catchup)
        self.assertEqual(self.dag.max_active_runs, 1)
        self.assertEqual(self.dag.max_active_tasks, 1)

    def test_tasks_follow_resource_schema_load_verify_summary_order(self):
        tasks = {task.task_id: task for task in self.dag.tasks}
        self.assertEqual(set(tasks), {
            "validate_run", "check_resources", "prepare_stg_schema",
            "load_kotlin", "verify_kotlin", "load_ktor", "verify_ktor",
            "load_nowinandroid", "verify_nowinandroid", "summarize",
        })
        chain = (
            "validate_run", "check_resources", "prepare_stg_schema",
            "load_kotlin", "verify_kotlin", "load_ktor", "verify_ktor",
            "load_nowinandroid", "verify_nowinandroid", "summarize",
        )
        for upstream, downstream in zip(chain, chain[1:]):
            with self.subTest(upstream=upstream, downstream=downstream):
                self.assertIn(upstream, tasks[downstream].upstream_task_ids)

    def test_params_restrict_user_choices(self):
        params = self.dag.params
        self.assertEqual(params["repository"], "Kotlin/kotlinx.coroutines")
        self.assertEqual(params["mode"], "backfill")
        self.assertEqual(params["max_issues"], 8)
        self.assertIs(params["full_load"], False)
        self.assertIn("all", params.get_param("repository").schema["enum"])
        self.assertIn("audit", params.get_param("mode").schema["enum"])


if __name__ == "__main__":
    unittest.main()

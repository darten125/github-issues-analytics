"""Manual, serial Airflow orchestration for the existing GitHub Issues STG loader."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.sdk import Param, dag, get_current_context, task

from src.stg import airflow_adapter
from src.stg.config import DEFAULT_CREATED_SINCE, REPOSITORIES


@dag(
    dag_id="github_issues_stg",
    description="Manually load and verify GitHub Issues STG, one repository at a time",
    schedule=None,
    catchup=False,
    start_date=datetime(2023, 1, 1, tzinfo=timezone.utc),
    max_active_runs=1,
    max_active_tasks=1,
    params={
        "repository": Param(
            REPOSITORIES[0], type="string", enum=[*REPOSITORIES, "all"],
            description="One repository or all three in fixed order",
        ),
        "mode": Param(
            "backfill", type="string", enum=["backfill", "sync", "audit"],
            description="backfill resumes, sync checks updated_at, audit rereads events",
        ),
        "created_since": Param(
            DEFAULT_CREATED_SINCE, type="string", format="date-time",
            description="Inclusive Issue creation cutoff with time zone",
        ),
        "max_issues": Param(
            8, type=["integer", "null"], minimum=1,
            description="Per-repository sample limit; set null only with full_load=true",
        ),
        "full_load": Param(
            False, type="boolean",
            description="Explicitly allow a full scan; requires max_issues=null",
        ),
    },
    tags=["github", "stg", "manual"],
)
def github_issues_stg():
    @task(task_id="validate_run", retries=0)
    def validate_run():
        return airflow_adapter.normalize_run_params(get_current_context()["params"])

    @task(task_id="check_resources", retries=1, retry_delay=timedelta(minutes=1))
    def check_resources(_settings):
        return airflow_adapter.check_resources()

    @task(task_id="prepare_stg_schema", retries=1, retry_delay=timedelta(minutes=1))
    def prepare_stg_schema(_resources_ready):
        return airflow_adapter.prepare_schema()

    @task(retries=1, retry_delay=timedelta(minutes=15))
    def load_one(settings, full_name):
        return airflow_adapter.load_selected_repository(settings, full_name)

    @task(retries=0)
    def verify_one(settings, outcome):
        return airflow_adapter.verify_batch(settings, outcome)

    @task(task_id="summarize", retries=0)
    def summarize(settings, first, second, third):
        return airflow_adapter.build_summary(settings, first, second, third)

    settings = validate_run()
    resources_ready = check_resources(settings)
    schema_ready = prepare_stg_schema(resources_ready)

    first_load = load_one.override(task_id="load_kotlin")(settings, REPOSITORIES[0])
    schema_ready >> first_load
    first_checked = verify_one.override(task_id="verify_kotlin")(settings, first_load)

    second_load = load_one.override(task_id="load_ktor")(settings, REPOSITORIES[1])
    first_checked >> second_load
    second_checked = verify_one.override(task_id="verify_ktor")(settings, second_load)

    third_load = load_one.override(task_id="load_nowinandroid")(settings, REPOSITORIES[2])
    second_checked >> third_load
    third_checked = verify_one.override(task_id="verify_nowinandroid")(settings, third_load)

    summarize(settings, first_checked, second_checked, third_checked)


github_issues_stg()

"""Airflow DAG: NYC Taxi lakehouse — incremental monthly pipeline.

Each run loads exactly one month, selected from the run's logical date, through
bronze -> silver -> gold, then runs dbt. The **monthly schedule with catchup is
what drives incremental loading**: unpausing the DAG backfills 2024-01, 2024-02,
… one month per run, each appending a new Iceberg snapshot.

Tasks are BashOperators that call the same scripts you'd run by hand
(`python scripts/ingest_*.py --month YYYY-MM`), so each layer runs in its own
process and the bronze/silver steps are idempotent if a run is retried.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# The project is mounted here by docker-compose.yaml.
PROJECT_DIR = "/opt/airflow/project"

# Pipeline deps live in an isolated virtualenv (see airflow/Dockerfile) because
# PyIceberg needs SQLAlchemy 2.0 while Airflow 2.10 pins 1.4. The tasks call this
# venv's python/dbt directly so Airflow's own environment is left untouched.
PY = "/opt/pipeline-venv/bin/python"
DBT = "/opt/pipeline-venv/bin/dbt"

# Month being processed, derived from the run's data interval. For an @monthly
# schedule, data_interval_start is the first day of the month the run covers.
MONTH = "{{ data_interval_start.strftime('%Y-%m') }}"

# Whether to run the dbt steps. Set RUN_DBT=false in the environment to skip them
# (e.g. if BigQuery isn't configured in the container yet).
RUN_DBT = os.environ.get("RUN_DBT", "true").lower() == "true"

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="nyc_taxi_lakehouse",
    description="Incremental monthly NYC taxi lakehouse: bronze -> silver -> gold -> dbt",
    schedule="@monthly",
    start_date=datetime(2024, 1, 1),
    # Bound the schedule to the months actually loaded (Jan–Oct 2024). With
    # catchup, unpausing only ever (re)runs Jan–Oct — it can't wander into
    # not-yet-loaded months. Bump this when you load more.
    end_date=datetime(2024, 10, 31),
    catchup=True,
    max_active_runs=1,  # serialize months: single SQLite-catalog writer, ordered loads
    default_args=default_args,
    tags=["lakehouse", "iceberg", "nyc-taxi"],
) as dag:

    ingest_bronze = BashOperator(
        task_id="ingest_bronze",
        bash_command=f"cd {PROJECT_DIR} && {PY} scripts/ingest_bronze.py --month {MONTH}",
    )

    ingest_silver = BashOperator(
        task_id="ingest_silver",
        bash_command=f"cd {PROJECT_DIR} && {PY} scripts/ingest_silver.py --month {MONTH}",
    )

    ingest_gold = BashOperator(
        task_id="ingest_gold",
        bash_command=f"cd {PROJECT_DIR} && {PY} scripts/ingest_gold.py",
    )

    ingest_bronze >> ingest_silver >> ingest_gold

    if RUN_DBT:
        dbt_run = BashOperator(
            task_id="dbt_run",
            bash_command=f"cd {PROJECT_DIR}/dbt_nyc_taxi && {DBT} run",
        )
        dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=f"cd {PROJECT_DIR}/dbt_nyc_taxi && {DBT} test",
        )
        ingest_gold >> dbt_run >> dbt_test

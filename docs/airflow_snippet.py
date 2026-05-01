"""Airflow integration sketch (NOT executed — illustrative).

Drop this in your Airflow `dags/` folder. The pipeline already exposes a
clean entry point (`python -m pipeline.cli run`) so we treat each stage as
a BashOperator. The advantage of three tasks (vs one) is per-stage retries
and a clearer Gantt view in the UI.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
    "depends_on_past": False,
}

with DAG(
    dag_id="ecommerce_events_etl",
    start_date=datetime(2026, 4, 1),
    schedule="0 * * * *",          # hourly
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,             # idempotent but serialize for predictable cost
    tags=["etl", "pyspark", "delta"],
) as dag:

    bronze = BashOperator(
        task_id="bronze",
        bash_command="python -m pipeline.cli run --skip-silver --skip-gold --no-quality",
    )
    silver = BashOperator(
        task_id="silver",
        bash_command="python -m pipeline.cli run --skip-bronze --skip-gold",
    )
    gold = BashOperator(
        task_id="gold",
        bash_command="python -m pipeline.cli run --skip-bronze --skip-silver",
    )

    bronze >> silver >> gold

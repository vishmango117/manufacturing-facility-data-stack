"""dbt_build DAG — runs dbt staging → marts → test on a schedule.

Orchestrates the dbt pipeline:
1. dbt seed (machines.csv → dim_equipment)
2. dbt run (staging → marts)
3. dbt test (data quality checks)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

DAG_ARGS = {
    "dag_id": "dbt_build",
    "default_args": DEFAULT_ARGS,
    "description": "Run dbt staging + marts + tests",
    "schedule_interval": "@hourly",
    "start_date": datetime(2024, 1, 1),
    "catchup": False,
    "tags": ["dbt", "warehouse"],
}


with DAG(**DAG_ARGS) as dag:
    # Step 1: Seed the machines dimension from CSV
    seed_machines = BashOperator(
        task_id="dbt_seed",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt seed --profiles-dir . --profile acn_platform"
        ),
    )

    # Step 2: Run dbt models (staging → marts)
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt run --profiles-dir . --profile acn_platform"
        ),
    )

    # Step 3: Run dbt tests
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt test --profiles-dir . --profile acn_platform"
        ),
    )

    # Step 4: Generate dbt docs
    dbt_docs = BashOperator(
        task_id="dbt_docs",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt docs generate --profiles-dir . --profile acn_platform && "
            "dbt docs serve --port 8085 --profiles-dir . --profile acn_platform"
        ),
        do_xcom_push=False,
    )

    seed_machines >> dbt_run >> dbt_test >> dbt_docs

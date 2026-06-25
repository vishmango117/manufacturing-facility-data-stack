"""erp_api_batch DAG — batch refresh of ERP/MES master data.

Pulls from the FastAPI MES service to refresh master data in the warehouse.
Running CDC via Debezium handles operational changes; this DAG handles
periodic master-data reconciliation.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

DAG_ARGS = {
    "dag_id": "erp_api_batch",
    "default_args": DEFAULT_ARGS,
    "description": "Periodic ERP master data refresh via API",
    "schedule_interval": "0 */6 * * *",  # every 6 hours
    "start_date": datetime(2024, 1, 1),
    "catchup": False,
    "tags": ["erp", "api", "batch"],
}


with DAG(**DAG_ARGS) as dag:
    # Verify API connectivity
    api_health = BashOperator(
        task_id="api_health_check",
        bash_command=(
            "curl -sf http://erp-api:8000/health || "
            "exit 1"
        ),
    )

    # Refresh dim_equipment from machines table
    refresh_equipment = PostgresOperator(
        task_id="refresh_dim_equipment",
        postgres_conn_id="warehouse",
        sql="""
        TRUNCATE marts.dim_equipment RESTART IDENTITY;
        INSERT INTO marts.dim_equipment
        SELECT
            row_number() over (),
            "id", "name", type, building, "buildingCode",
            "energyTag", "bmsTag",
            "isactive" = 'true' OR "isactive" = 't' OR "isactive" = 1,
            createdby, createdon, modifiedby, modifiedon
        FROM erp_raw.machines
        WHERE type IN ('Chiller', 'AHU', 'Cooling Tower', 'Air Compressor', 'Air Coolers');
        """,
    )

    # Refresh dim_machine from manufacturing machines
    refresh_machine = PostgresOperator(
        task_id="refresh_dim_machine",
        postgres_conn_id="warehouse",
        sql="""
        TRUNCATE marts.dim_machine RESTART IDENTITY;
        INSERT INTO marts.dim_machine
        SELECT
            row_number() over (),
            machine_id, machine_type, building,
            case
                when building like 'Building-Alpha' then 'BA'
                when building like 'Building-Beta'  then 'BB'
                when building like 'Building-Gamma' then 'BG'
                else substr(building, 10, 2)
            end,
            energy_tag, rated_power_kw,
            case machine_type
                when 'INJECTION_MOULDING' then 1
                when 'CNC' then 2
                when 'HEATING' then 3
            end
        FROM erp_raw.machines
        WHERE machine_type IN ('INJECTION_MOULDING', 'CNC', 'HEATING');
        """,
    )

    # Refresh dim_product
    refresh_product = PostgresOperator(
        task_id="refresh_dim_product",
        postgres_conn_id="warehouse",
        sql="""
        TRUNCATE marts.dim_product RESTART IDENTITY;
        INSERT INTO marts.dim_product
        SELECT
            row_number() over (),
            id, name, sku, family, uom
        FROM erp_raw.products;
        """,
    )

    api_health >> [refresh_equipment, refresh_machine, refresh_product]

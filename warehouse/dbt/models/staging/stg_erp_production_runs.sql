{{ config(materialized='view') }}

{#- stg_erp_production_runs — production run records from ERP CDC. Returns empty when no CDC data yet. -#}

{% if adapter.get_relation(database=target.database, schema='erp_raw', identifier='production_runs') %}
select
    id,
    work_order_id,
    machine_id,
    good_qty,
    scrap_qty,
    total_qty,
    start_time,
    end_time,
    shift_id
from erp_raw.production_runs
{% else %}
select
    null::integer   as id,
    null::integer   as work_order_id,
    null::text      as machine_id,
    null::integer   as good_qty,
    null::integer   as scrap_qty,
    null::integer   as total_qty,
    null::timestamp as start_time,
    null::timestamp as end_time,
    null::text      as shift_id
where false
{% endif %}

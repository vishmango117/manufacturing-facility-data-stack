{{ config(materialized='view') }}

{{
  /*
   * stg_erp_production_runs — production run records from CDC.
   */
}}

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

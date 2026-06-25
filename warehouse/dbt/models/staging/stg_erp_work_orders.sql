{{ config(materialized='view') }}

{{
  /*
   * stg_erp_work_orders — dedupe work orders to latest per PK.
   */
}}

with ranked as (
    select
        *,
        row_number() over (
            partition by id
            order by modifiedon desc nulls last
        ) as rn
    from erp_raw.work_orders
)

select
    id,
    order_no,
    product_id,
    machine_id,
    planned_qty,
    actual_qty,
    planned_start,
    planned_end,
    actual_start,
    actual_end,
    status,
    shift_id
from ranked
where rn = 1

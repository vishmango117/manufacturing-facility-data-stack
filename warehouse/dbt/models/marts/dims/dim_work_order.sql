{{ config(materialized='table') }}

{{
  /*
   * dim_work_order — work order dimension from ERP/MES.
   * SCD Type 1: status changes are reflected in latest row.
   */
}}

select
    row_number() over ()                                as work_order_key,
    id                                                  as work_order_id,
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
from erp_raw.work_orders

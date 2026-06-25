{{ config(materialized='table') }}

{{
  /*
   * fact_production — production fact from ERP/MES.
   *
   * Grain: 1 row per work order per machine per status-change interval.
   * Measures: good_qty, scrap_qty, actual duration, OEE components.
   */
}}

with work_orders as (
    select * from {{ ref('dim_work_order') }}
),

production_runs as (
    select * from erp_raw.production_runs
),

machine_states as (
    select * from erp_raw.machine_states
),

combined as (
    select
        wo.work_order_key,
        wo.product_id,
        wo.machine_id,
        wo.shift_id,
        wo.status,
        pr.good_qty,
        pr.scrap_qty,
        pr.total_qty,
        ms.state,
        ms.start_time as state_start,
        ms.end_time as state_end,
        extract(epoch from (ms.end_time - ms.start_time)) / 60.0 as duration_minutes
    from work_orders wo
    left join production_runs pr on wo.work_order_id = pr.work_order_id
    left join machine_states ms on wo.machine_id = ms.machine_id
)

select * from combined

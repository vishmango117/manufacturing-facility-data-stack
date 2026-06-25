{{ config(materialized='table') }}

{#- fact_production — production fact from ERP/MES work orders and production runs. -#}

with work_orders as (
    select * from {{ ref('dim_work_order') }}
),

dim_machine as (
    select * from {{ ref('dim_machine') }}
),

production_runs as (
    select * from {{ ref('stg_erp_production_runs') }}
),

combined as (
    select
        wo.work_order_key,
        m.machine_key,
        wo.work_order_id,
        wo.product_id,
        wo.machine_id,
        wo.planned_qty,
        wo.status,
        pr.good_qty,
        pr.scrap_qty,
        pr.total_qty,
        pr.start_time,
        pr.end_time,
        extract(epoch from (pr.end_time - pr.start_time)) / 60.0 as duration_minutes
    from work_orders wo
    left join dim_machine m on wo.machine_id = m.machine_id
    left join production_runs pr on wo.work_order_id = pr.work_order_id
)

select * from combined

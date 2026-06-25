{{ config(materialized='table') }}

{#- dim_work_order — work order dimension from ERP/MES. SCD Type 1: latest status reflected. -#}

select
    row_number() over ()                                as work_order_key,
    work_order_id,
    order_no,
    product_id,
    machine_id,
    planned_qty,
    due_date,
    status
from erp_raw.work_orders

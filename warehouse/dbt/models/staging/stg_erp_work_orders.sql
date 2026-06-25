{{ config(materialized='view') }}

with ranked as (
    select
        *,
        row_number() over (
            partition by work_order_id
            order by updated_at desc nulls last
        ) as rn
    from erp_raw.work_orders
)

select
    work_order_id,
    order_no,
    product_id,
    machine_id,
    planned_qty,
    due_date,
    status
from ranked
where rn = 1

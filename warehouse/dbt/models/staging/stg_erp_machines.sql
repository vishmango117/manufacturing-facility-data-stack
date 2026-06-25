{{ config(materialized='view') }}

with ranked as (
    select
        *,
        row_number() over (
            partition by machine_id
            order by __source_ts_ms desc nulls last
        ) as rn
    from erp_raw.machines
    where machine_type in ('INJECTION_MOULDING', 'CNC', 'HEATING')
)

select
    machine_id,
    machine_type,
    building,
    energy_tag,
    rated_power_kw,
    is_active
from ranked
where rn = 1

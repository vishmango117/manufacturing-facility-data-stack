{{ config(materialized='view') }}

{#- stg_telemetry_ahu — AHU-specific wide view with derived metrics. -#}

with stg_telemetry as (
    select * from {{ ref('stg_telemetry') }}
),

filtered as (
    select *
    from stg_telemetry
    where equipment_type = 'AHU'
),

with_derived as (
    select
        *,
        coalesce(ahu_return_temp::numeric - ahu_supply_temp::numeric, 0) as delta_temp,
        case
            when ahu_supply_temp is not null
                 and ahu_supply_temp::numeric between 10 and 20 then 'OK'
            when ahu_supply_temp is not null then 'WARN'
            else 'MISSING'
        end as supply_temp_status
    from filtered
)

select * from with_derived

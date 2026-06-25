{{ config(materialized='table') }}

{#- fact_energy_reading — manufacturing machine energy fact at 1-minute grain. Grain: 1 row per machine per minute. Measures: totalPower, totalEnergy, delta_kWh. Joined to dim_machine via energyTag. -#}

with stg as (
    select * from {{ ref('stg_telemetry') }}
),

dim_machine as (
    select * from {{ ref('dim_machine') }}
),

dim_date as (
    select * from {{ ref('dim_date') }}
),

dim_time as (
    select * from {{ ref('dim_time') }}
),

filtered as (
    select
        m.machine_key,
        d.date_id,
        t.time_key,
        m.machine_id,
        m.machine_type,
        m.building,
        m.building_code,
        m.energy_tag,
        cast(s.total_power as numeric) as total_power,
        cast(s.total_energy as numeric) as total_energy,
        case
            when lag(s.total_energy) over (
                partition by s.device_id order by s."time"
            ) is not null then
                cast(s.total_energy as numeric) - lag(cast(s.total_energy as numeric)) over (
                    partition by s.device_id order by s."time"
                )
            else null
        end as delta_kwh
    from stg s
    join dim_machine m on s.device_name = m.energy_tag
    join dim_date d on s."time"::date = d.date_id
    join dim_time t on date_part('hour', s."time") * 60 + date_part('minute', s."time") = t.time_key
    where s.equipment_type in ('INJECTION_MOULDING', 'CNC', 'HEATING')
)

select * from filtered

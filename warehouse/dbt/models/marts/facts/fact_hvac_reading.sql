{{ config(materialized='table') }}

{#- fact_hvac_reading — HVAC telemetry fact at 1-minute grain joined to dim_equipment via bmstag. -#}

with ahu_stg as (
    select * from {{ ref('stg_telemetry_ahu') }}
),

chiller_stg as (
    select * from {{ ref('stg_telemetry_chiller') }}
),

dim_equipment as (
    select * from {{ ref('dim_equipment') }}
),

dim_date as (
    select * from {{ ref('dim_date') }}
),

dim_time as (
    select * from {{ ref('dim_time') }}
),

ahu as (
    select
        e.equipment_key,
        d.date_id,
        t.time_key,
        s.device_name,
        s.building,
        s.equipment_type,
        s.ahu_supply_temp::numeric   as supply_temp,
        s.ahu_return_temp::numeric   as return_temp,
        s.ahu_supply_rh::numeric     as supply_rh,
        s.ahu_return_rh::numeric     as return_rh,
        s.ahu_supply_flow::numeric   as supply_flow,
        s.delta_temp                 as delta_temp,
        s.supply_temp_status,
        null::numeric                as cws_temp,
        null::numeric                as cwr_temp
    from ahu_stg s
    join dim_equipment e on s.device_name = e.bmstag
    join dim_date d on s."time"::date = d.date_id
    join dim_time t
        on date_part('hour', s."time") * 60 + date_part('minute', s."time") = t.time_key
),

chiller as (
    select
        e.equipment_key,
        d.date_id,
        t.time_key,
        s.device_name,
        s.building,
        s.equipment_type,
        null::numeric                as supply_temp,
        null::numeric                as return_temp,
        null::numeric                as supply_rh,
        null::numeric                as return_rh,
        null::numeric                as supply_flow,
        s.delta_temp                 as delta_temp,
        s.supply_temp_status,
        s.chiller_cws_temp::numeric  as cws_temp,
        s.chiller_cwr_temp::numeric  as cwr_temp
    from chiller_stg s
    join dim_equipment e on s.device_name = e.bmstag
    join dim_date d on s."time"::date = d.date_id
    join dim_time t
        on date_part('hour', s."time") * 60 + date_part('minute', s."time") = t.time_key
)

select * from ahu
union all
select * from chiller

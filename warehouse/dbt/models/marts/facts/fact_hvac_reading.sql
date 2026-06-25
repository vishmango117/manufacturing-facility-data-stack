{{ config(materialized='table') }}

{{
  /*
   * fact_hvac_reading — HVAC telemetry fact at 1-minute grain.
   *
   * Wide per equipment-type facts are recommended for clean Metabase semantics.
   * This model produces a unified view per equipment type, joined to
   * dim_equipment via bmsTag (BMS device name).
   */
}}

with stg as (
    select * from {{ ref('stg_telemetry') }}
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

-- AHU readings
ahu as (
    select
        e.equipment_key,
        d.date_id,
        t.time_key,
        s.ahu_supply_temp,
        s.ahu_return_temp,
        s.ahu_supply_rh,
        s.ahu_return_rh,
        s.ahu_supply_flow,
        s.delta_temp,
        s.supply_temp_status,
        s.device_name,
        s.building,
        s.equipment_type,
        s.total_power,
        s.total_energy
    from stg s
    join dim_equipment e on s.device_name = e.bmsTag
    join dim_date d on s."time"::date = d.date_id
    join dim_time t on date_part('hour', s."time") * 60 + date_part('minute', s."time") = t.minute_of_day
    where s.equipment_type = 'AHU'
),

-- Chiller readings
chiller as (
    select
        e.equipment_key,
        d.date_id,
        t.time_key,
        s.chiller_cws_temp,
        s.chiller_cwr_temp,
        s.chiller_cds_temp,
        s.chiller_cdr_temp,
        s.delta_temp,
        s.delta_pct,
        s.supply_temp_status,
        s.device_name,
        s.building,
        s.equipment_type,
        s.total_power,
        s.total_energy
    from stg s
    join dim_equipment e on s.device_name = e.bmsTag
    join dim_date d on s."time"::date = d.date_id
    join dim_time t on date_part('hour', s."time") * 60 + date_part('minute', s."time") = t.minute_of_day
    where s.equipment_type = 'Chillers'
),

-- Combined HVAC fact
combined as (
    select * from ahu
    union all
    select * from chiller
)

select * from combined

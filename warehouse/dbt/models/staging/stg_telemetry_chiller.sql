{{ config(materialized='view') }}

{{
  /*
   * stg_telemetry_chiller — Chiller-specific wide view with derived COP proxy.
   */
}}

with stg_telemetry as (
    select * from {{ ref('stg_telemetry') }}
),

filtered as (
    select *
    from stg_telemetry
    where equipment_type = 'Chillers'
),

with_derived as (
    select
        *,
        coalesce(cwr_temp - cws_temp, 0) as delta_temp,
        case
            when cwr_temp is not null and cws_temp is not null and cws_temp > 0
                then round((cwr_temp - cws_temp) / cws_temp::numeric * 100, 2)
            else null
        end as delta_pct,
        case
            when cws_temp is not null and cws_temp between 4 and 12 then 'OK'
            when cws_temp is not null then 'WARN'
            else 'MISSING'
        end as supply_temp_status
    from filtered
)

select * from with_derived

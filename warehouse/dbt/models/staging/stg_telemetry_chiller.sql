{{ config(materialized='view') }}

{#- stg_telemetry_chiller — Chiller-specific wide view with derived COP proxy. -#}

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
        coalesce(chiller_cwr_temp::numeric - chiller_cws_temp::numeric, 0) as delta_temp,
        case
            when chiller_cwr_temp is not null and chiller_cws_temp is not null
                 and chiller_cws_temp::numeric > 0
                then round(
                    (chiller_cwr_temp::numeric - chiller_cws_temp::numeric)
                    / chiller_cws_temp::numeric * 100, 2
                )
            else null
        end as delta_pct,
        case
            when chiller_cws_temp is not null
                 and chiller_cws_temp::numeric between 4 and 12 then 'OK'
            when chiller_cws_temp is not null then 'WARN'
            else 'MISSING'
        end as supply_temp_status
    from filtered
)

select * from with_derived

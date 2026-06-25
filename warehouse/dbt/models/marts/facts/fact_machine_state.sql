{{ config(materialized='table') }}

{#- fact_machine_state — machine state intervals for downtime analysis. Grain: 1 row per machine per state interval (RUN/IDLE/DOWN). Sourced from ERP machine_states table via CDC. -#}

with machine_states as (
    select * from {{ ref('stg_erp_machine_states') }}
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

joined as (
    select
        m.machine_key,
        d.date_id,
        t.time_key,
        ms.machine_id,
        ms.state,
        ms.start_time,
        ms.end_time,
        extract(epoch from (ms.end_time - ms.start_time)) / 60.0 as duration_minutes
    from machine_states ms
    join dim_machine m on ms.machine_id = m.machine_id
    join dim_date d on ms.start_time::date = d.date_id
    join dim_time t on date_part('hour', ms.start_time) * 60 + date_part('minute', ms.start_time) = t.time_key
)

select * from joined

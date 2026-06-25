{{ config(materialized='table') }}

{{
  /*
   * dim_time — time-of-day at minute grain (0–1439).
   * Used for 1-minute grain telemetry facts.
   */
}}

with minutes as (
    select generate_series(0, 1439) as minute_of_day
)

select
    minute_of_day                                       as time_key,
    minute_of_day / 60                                  as hour_of_day,
    minute_of_day % 60                                  as minute_of_hour,
    lpad((minute_of_day / 60)::text, 2, '0') || ':' ||
    lpad((minute_of_day % 60)::text, 2, '0')            as time_label,
    case
        when minute_of_day / 60 between 6 and 11 then 'morning'
        when minute_of_day / 60 between 12 and 17 then 'afternoon'
        when minute_of_day / 60 between 18 and 21 then 'evening'
        else 'night'
    end                                                 as time_of_day
from minutes

{{ config(materialized='table') }}

{{
  /*
   * dim_date — calendar dimension at day grain.
   * Generated from a date range covering the facility data.
   */
}}

with dates as (
    select generate_series(
        '2024-01-01'::date,
        '2026-12-31'::date,
        interval '1 day'
    ) as date_day
)

select
    date_day                                            as date_id,
    date_day                                            as full_date,
    extract(dow from date_day)                          as day_of_week,
    extract(isodow from date_day)                       as day_of_week_iso,
    to_char(date_day, 'Day')                            as day_name,
    extract(week from date_day)                         as week_of_year,
    extract(month from date_day)                        as month_of_year,
    to_char(date_day, 'Month')                          as month_name,
    extract(quarter from date_day)                      as quarter,
    extract(year from date_day)                         as year,
    (extract(month from date_day) - 1) / 3 + 1          as quarter_of_year
from dates

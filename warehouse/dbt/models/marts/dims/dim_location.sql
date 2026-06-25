{{ config(materialized='table') }}

{{
  /*
   * dim_location — building hierarchy from building names.
   * Derives building code (BA/BB/BG) from building name.
   */
}}

select distinct
    building,
    case
        when building like 'Building-Alpha' then 'BA'
        when building like 'Building-Beta'  then 'BB'
        when building like 'Building-Gamma' then 'BG'
        else substr(building, 10, 2)
    end                                                 as building_code,
    case
        when building like 'Building-Alpha' then 1
        when building like 'Building-Beta'  then 2
        when building like 'Building-Gamma' then 3
        else 9
    end                                                 as building_order,
    'Facility'                                          as campus
from {{ ref('stg_telemetry') }}
where building is not null

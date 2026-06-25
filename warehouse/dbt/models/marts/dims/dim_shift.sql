{{ config(materialized='table') }}

{{
  /*
   * dim_shift — shift dimension from ERP.
   */
}}

select
    row_number() over ()                                as shift_key,
    id                                                  as shift_id,
    shift_code,
    start_time,
    end_time,
    crew
from erp_raw.shifts

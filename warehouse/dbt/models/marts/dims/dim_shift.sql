{{ config(materialized='table') }}

{#- dim_shift — shift dimension from ERP. -#}

select
    row_number() over ()                                as shift_key,
    shift_id,
    shift_code,
    start_hour,
    end_hour,
    crew
from erp_raw.shifts

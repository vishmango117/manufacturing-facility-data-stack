{{ config(materialized='table') }}

{{
  /*
   * dim_machine — manufacturing machine dimension.
   *
   * Sourced from the ERP machines table (via CDC → erp_raw.machines).
   * Covers Injection Moulding, CNC, and Heating production assets.
   * The energy_tag joins to EMS telemetry (energyTag).
   */
}}

select
    row_number() over ()                                as machine_key,
    machine_id,
    machine_type,
    building,
    case
        when building like 'Building-Alpha' then 'BA'
        when building like 'Building-Beta'  then 'BB'
        when building like 'Building-Gamma' then 'BG'
        else substr(building, 10, 2)
    end                                                 as building_code,
    energy_tag,
    rated_power_kw,
    case machine_type
        when 'INJECTION_MOULDING' then 1
        when 'CNC' then 2
        when 'HEATING' then 3
    end                                                 as machine_type_order
from erp_raw.machines
where machine_type in ('INJECTION_MOULDING', 'CNC', 'HEATING')

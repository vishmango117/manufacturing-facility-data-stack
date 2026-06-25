{{ config(materialized='table') }}

{#- dim_equipment — HVAC equipment dimension, seeded from machines.csv. The bmsTag column joins to BMS telemetry device names. The energyTag column joins to EMS energy telemetry names. This is the conformed asset dimension for cross-domain analytics. -#}

select
    row_number() over ()                                as equipment_key,
    id,
    name,
    type                                                as equipment_type,
    building,
    buildingcode                                        as building_code,
    energytag,
    bmstag,
    case
        when isactive::text in ('true', 't', '1') then true
        else false
    end                                                 as is_active,
    createdby,
    createdon,
    modifiedby,
    modifiedon
from {{ ref('machines') }}
where type in ('Chiller', 'AHU', 'Cooling Tower', 'Air Compressor', 'Air Coolers')

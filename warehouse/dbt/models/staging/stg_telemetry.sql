{{ config(materialized='view') }}

with base as (
    select
        "time",
        value,
        dimensions,
        metadata,
        device_id,
        source
    from {{ source('raw', 'telemetry') }}
),

-- Unpack value JSONB into typed columns per equipment type
unpacked as (
    select
        "time",
        device_id,
        source,
        dimensions->>'building'       as building,
        dimensions->>'equipmentType'  as equipment_type,
        dimensions->>'name'           as device_name,
        metadata->>'deviceId'         as metadata_device_id,
        metadata->>'topic'            as topic,
        metadata->>'schema_ver'       as schema_ver,

        -- Generic (all types)
        value->>'totalPower'          as total_power,
        value->>'totalEnergy'         as total_energy,

        -- AHU metrics
        value->>'Supply_Temp'         as ahu_supply_temp,
        value->>'Return_Temp'         as ahu_return_temp,
        value->>'Supply_RH'           as ahu_supply_rh,
        value->>'Return_RH'           as ahu_return_rh,
        value->>'Supply_Flow'         as ahu_supply_flow,

        -- Chiller metrics
        value->>'Chilled_Water_Supply_Temp'  as chiller_cws_temp,
        value->>'Chilled_Water_Return_Temp'  as chiller_cwr_temp,
        value->>'Condensor_Supply_Temp'      as chiller_cds_temp,
        value->>'Condensor_Return_Temp'      as chiller_cdr_temp,

        -- Chiller header metrics
        value->>'CHWS_Temp'            as ch_header_chws,
        value->>'CHWR_Temp'            as ch_header_chwr,
        value->>'CDWS_Temp'            as ch_header_cdws,
        value->>'CDWR_Temp'            as ch_header_cdwr,
        value->>'CHW_DPT'              as ch_header_chw_dpt,
        value->>'CW_DPT'               as ch_header_cw_dpt,

        -- Cooling tower header
        value->>'Outdoor_MIT_01_Temp'  as ct_outdoor_temp,
        value->>'Outdoor_MIT_01_RH'    as ct_outdoor_rh,

        -- Air cooler
        value->>'Supply_Temp'          as cooler_supply_temp,

        -- Air compressor
        value->>'Total_Flow'           as compressor_total_flow,

        -- Flow meters
        value->>'consumptionFlowRate'  as flow_consumption,
        value->>'CHW_PDT'              as ch_pdt,
        value->>'Chiller_Header_Supply_Temp' as ch_hdr_supply_temp,
        value->>'Chiller_Header_Return_Temp' as ch_hdr_return_temp

    from base
)

select * from unpacked

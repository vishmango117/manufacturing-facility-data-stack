-- Unified warehouse: Postgres + TimescaleDB
-- Creates extensions, schemas, the unified raw.telemetry hypertable, and the
-- erp_raw landing tables for the Debezium/Connect JDBC sink.

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS erp_raw;

-- --------------------------------------------------------------------------
-- Unified RAW landing (schema-on-read): one table for ALL BMS/EMS telemetry.
--   time        : event time (UTC)
--   value       : meter/sensor readings, e.g. {"CHWS_Temp": 6.8, "CHWR_Temp": 10.1}
--   dimensions  : {"building","equipmentType","name", ...}
--   metadata    : {"msgId","deviceId","source","topic","schema_ver", ...}
-- device_id/source are generated for partition pruning + compression segmentby.
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.telemetry (
    "time"      timestamptz      NOT NULL,
    value       jsonb            NOT NULL,
    dimensions  jsonb            NOT NULL,
    metadata    jsonb            NOT NULL,
    device_id   text GENERATED ALWAYS AS (metadata->>'deviceId') STORED,
    source      text GENERATED ALWAYS AS (metadata->>'source')   STORED
);

SELECT create_hypertable('raw.telemetry', 'time',
                         chunk_time_interval => INTERVAL '1 day',
                         if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_telemetry_dims_gin   ON raw.telemetry USING GIN (dimensions);
CREATE INDEX IF NOT EXISTS idx_telemetry_value_gin  ON raw.telemetry USING GIN (value);
CREATE INDEX IF NOT EXISTS idx_telemetry_device     ON raw.telemetry (device_id, "time" DESC);
CREATE INDEX IF NOT EXISTS idx_telemetry_source     ON raw.telemetry (source, "time" DESC);

-- Compression + retention (demo-friendly windows).
ALTER TABLE raw.telemetry SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device_id',
    timescaledb.compress_orderby   = '"time" DESC'
);
SELECT add_compression_policy('raw.telemetry', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_retention_policy('raw.telemetry',  INTERVAL '90 days', if_not_exists => TRUE);

-- --------------------------------------------------------------------------
-- ERP/MES CDC landing: the Connect JDBC sink AUTO-CREATES structured tables in
-- the erp_raw schema (one per source table: products, machines, work_orders,
-- production_runs, machine_states, shifts). dbt staging dedupes to latest/key.
-- The sink connects with ?currentSchema=erp_raw so tables land here.
-- --------------------------------------------------------------------------
-- (intentionally no table DDL — auto.create=true on the sink owns these)

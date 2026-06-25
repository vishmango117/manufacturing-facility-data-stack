# Manufacturing Facility Data Platform — Build Plan

## Build Status (2026-06-24)

| #   | Workstream                                                   | Status   |
| --- | ------------------------------------------------------------ | -------- |
| 1   | Foundation: compose, env, registry, Kafka platform           | **DONE** |
| 2   | BMS HVAC simulators + per-device publishers                  | **DONE** |
| 3   | EMS manufacturing-machine simulators + per-device publishers | **DONE** |
| 4   | ERP/MES source DB + FastAPI + Debezium CDC                   | **DONE** |
| 5   | PyFlink stream jobs → unified raw.telemetry                  | **DONE** |
| 6   | Warehouse init + dbt Kimball star schema                     | **DONE** |
| 7   | Airflow orchestration + Metabase/Grafana serving + README    | **DONE** |

All workstreams complete. Repository is ready for `docker compose up`.

### BMS/EMS Simulator Detail (2026-06-25)

Equipment-type-specific generators implemented in `common/generators.py`:

- **Chillers**: CHWS held at 7 °C setpoint; CHWR ΔT (3-8 °C) scales with diurnal load; condenser supply/return tracks ambient heat rejection.
- **AHU**: supply-air setpoint 14 °C; return air rises with occupancy; supply/return RH correlated; optional Supply_Flow with fan-stall fault.
- **CoolingTower MainHeader**: outdoor temp/RH with anti-correlated diurnal drift (hotter day → lower RH).
- **Chiller MainHeader**: all 8 metrics correlated (CHW/CW temps + differential pressures + flow rate).
- **Air Compressors**: staged flow with 5-minute step-changes (4 capacity stages).
- **Air Coolers**: mild supply-temp drift following zone thermal load.
- **EMS (all 3 types)**: richer cycle phases — injection moulding clamp/inject/hold/eject, CNC 6-pass stepped spindle, heating ramp/soak/overshoot-correction/cool sawtooth.
- Validated: all 57 devices (39 BMS + 18 EMS) produce typed floats in realistic ranges.

## Context

The facility needs a single, scalable analytics platform that unifies four operational
data sources — **BMS** (HVAC facility data), **EMS** (energy data), **MES** and **ERP**
(production/scheduling) — into one Postgres warehouse modeled with **Kimball star-schema
dimensional modeling**. Today these systems are siloed; there is no common model to
correlate energy consumption against HVAC operation, machine state, and production output
(e.g. energy cost per unit produced, OEE vs. facility load).

This plan delivers a **containerized, platform-agnostic** reference data stack that:

- Simulates realistic field devices over **Modbus TCP** for BMS + EMS at a 1-minute cadence.
- Streams BMS/EMS telemetry through **Apache Kafka** (Avro + Schema Registry).
- Captures ERP/MES changes via **Debezium CDC** into the same Kafka backbone.
- Processes streams with **PyFlink** into **Postgres + TimescaleDB**.
- Builds the Kimball star schema with **dbt**, orchestrated by **Airflow**.
- Serves analytics via **Metabase** (star-schema BI) + **Grafana** (real-time ops).

Everything runs under a single **Docker Compose** stack (profiles per layer).

---

## Confirmed Technology Decisions

| Concern                  | Choice                                                                                                      |
| ------------------------ | ----------------------------------------------------------------------------------------------------------- |
| Field protocol (BMS/EMS) | Modbus TCP, simulated, 1-min interval (`pymodbus`)                                                          |
| Streaming backbone       | Apache Kafka (KRaft mode, no ZooKeeper)                                                                     |
| Serialization            | **Avro + Confluent Schema Registry**                                                                        |
| ERP/MES ingest           | Postgres source DB + REST API; **Debezium** CDC → Kafka                                                     |
| Stream processing        | **PyFlink** (event-time windowing) → Postgres                                                               |
| Warehouse                | **Postgres + TimescaleDB** (hypertables for raw telemetry, star schema for marts)                           |
| Transform / modeling     | **dbt** (Kimball dims + facts)                                                                              |
| Orchestration            | **Apache Airflow** (dbt runs + API batch pulls)                                                             |
| Data quality             | **dbt tests only** (not_null/unique/relationships/accepted_values), with docs for future Great Expectations |
| BI / serving             | **Metabase** (containerized) on the star schema + **Grafana** on TimescaleDB                                |
| Deployment               | **Docker Compose** with profiles                                                                            |

---

## Target Architecture & Data Flow

```
        ┌──────────────── EDGE / FIELD — one IoT stream PER MACHINE ───────────────┐
        │  Each device = its own Modbus TCP slave + its own publisher (raw IoT):    │
        │   BMS: every AHU / Chiller / CoolingTower / AirCompressor (per asset)     │
        │   EMS: every production machine — Injection Moulding, CNC, Heating (asset)│
        └───────────────────────────────────┬──────────────────────────────────────┘
                                             │ each publisher polls its own device @60s
                        ┌────────────────────▼──────────────────────┐
                        │  Modbus→Kafka publisher (1 per machine)    │
                        │  decode → Avro → produce keyed by device   │
                        │  (msg carries deviceId so streams stay     │
                        │   independent / raw, not pre-aggregated)   │
                        └────────────────────┬──────────────────────┘
                                             ▼
 ERP/MES Postgres ──Debezium CDC──▶  ┌──────────────────┐
 (work orders, products, runs)        │   APACHE KAFKA   │  + Schema Registry
                                      │  (KRaft, Avro)   │
                                      └───────┬──────────┘
                                              ▼
                                   ┌─────────────────────┐
                                   │   PyFlink jobs      │  decode Avro, validate,
                                   │ (event-time window) │  enrich, 1-min aggregate
                                   └──────────┬──────────┘
                                              ▼
                       ┌──────────────────────────────────────────┐
                       │  POSTGRES + TimescaleDB (unified warehouse)│
                       │  RAW  : unified JSONB hypertable           │
                       │         (time, value, dimensions, metadata)│
                       │  CDC  : erp_raw.* (Debezium sink)          │
                       │  ── dbt (Airflow-scheduled) ──▶            │
                       │  STAGING : stg_* views (unpack JSONB)      │
                       │  MARTS   : dim_* / fact_* (Kimball star)   │
                       └───────────────┬───────────────┬───────────┘
                                       ▼               ▼
                                  Metabase         Grafana
                            (star schema dashboards (real-time ops on
                             & self-serve reporting) TimescaleDB raw)
```

---

## Design Principle — Per-Machine IoT Streams

This is a **raw IoT** architecture, **not** a single BMS/EMS historian export. Therefore:

- **Every machine is its own independent stream/publisher.** Each device runs as its own
  Modbus TCP slave and has its own publisher process that emits only that device's telemetry —
  mirroring real field IoT where each asset reports independently. No upstream aggregation.
- Every message carries `deviceId` (and `source`), and is **produced keyed by `deviceId`** so a
  given machine's events stay ordered on one partition and consumers can fan out per device.
- **Topic strategy:** group by domain/equipment-type topics **partitioned/keyed by device**
  (e.g. `bms.hvac.chiller`, `ems.machine.injection_moulding`) rather than one giant topic — this
  keeps "one logical stream per machine" while avoiding the operational cost of thousands of
  physical topics. (Per-device topics documented as an alternative in Future Work.)
- **EMS now represents manufacturing machine energy** (production assets), distinct from the
  HVAC facility equipment in BMS.

## Sample Data Alignment (`samples/`)

The provided samples define the real shapes the simulators and warehouse must reproduce:

- **`bms.csv`** — `timebucket, name, building, equipmentType, value` where `value` is a JSON
  metric map. `equipmentType` ∈ {`AHU`, `Chillers`, `Chiller MainHeader`, `Air Coolers`,
  `CoolingTower MainHeader`, `Air Compressors`}. Metric keys vary by type, e.g.
  AHU `{Return_Temp, Return_RH, Supply_Temp, Supply_RH[, Supply_Flow]}`,
  Chillers `{Chilled_Water_Supply_Temp, Chilled_Water_Return_Temp, Condensor_Supply_Temp, Condensor_Return_Temp}`,
  Chiller header `{CHWS_Temp, CHWR_Temp, CDWS_Temp, CDWR_Temp, CHW_DPT, ...}`.
- **`ems.csv`** — `timebucket, name, building, totalEnergy, totalPower` (one energy + power
  reading per device per bucket). This defines the **measure shape**; for the project, EMS is
  populated by **manufacturing machines** — Injection Moulding, CNC, and Heating machines (each a
  distinct production asset across buildings/floors) — each emitting its own `{totalEnergy,totalPower}`
  IoT stream. These same machines are the production assets scheduled by MES/ERP.
- **`machines.csv`** — equipment master / **conformed dimension**:
  `id, name, type, building, buildingCode, energyTag, bmsTag, isactive, …`. Crucially,
  **`bmsTag` joins to BMS `name`** and **`energyTag` joins to EMS `name`** — this bridge is
  what lets a single physical asset (e.g. `BA-CHILLER-01`) tie its HVAC telemetry to its energy
  consumption. `type` ∈ {Chiller, Air Compressor, Cooling Tower, AHU}.

> Note: sample timestamps are **hourly** aggregates; the live simulators emit at the **1-minute**
> cadence required by the spec, using these same names/tags/metric keys.

## Unified RAW Landing Schema (TimescaleDB)

All BMS/EMS telemetry lands in **one generic, schema-on-read hypertable** (`raw.telemetry`),
so new device types/metrics need no DDL change — the gateways/Flink just emit a different
JSONB payload. Columns map directly onto the sample fields:

| Column       | Type                                | Source from samples                              | Example (BMS chiller / EMS)                                                                           |
| ------------ | ----------------------------------- | ------------------------------------------------ | ----------------------------------------------------------------------------------------------------- |
| `time`       | `timestamp without time zone` (UTC) | `timebucket`                                     | `2024-01-07 23:00:00`                                                                                 |
| `value`      | `jsonb`                             | BMS `value` map / EMS `{totalEnergy,totalPower}` | `{"CHWS_Temp":6.82,"CHWR_Temp":10.07}` · `{"totalEnergy":193.1,"totalPower":201.2}`                   |
| `dimensions` | `jsonb`                             | `building`, `equipmentType`/type, device `name`  | `{"building":"Building-Alpha","equipmentType":"Chillers","name":"BA-Chiller01"}`                      |
| `metadata`   | `jsonb`                             | provenance / lineage / join tags                 | `{"msgId":"…","deviceId":"BA-Chiller01","source":"bms","topic":"bms.hvac.chiller","schema_ver":"v1"}` |

Notes:

- Hypertable partitioned on `time`; add **GIN indexes** on `dimensions` (and `value` if filtered)
  for fast key lookups; apply compression + retention policies on older chunks.
- One physical table serves BMS **and** EMS; `metadata.source` (`bms`|`ems`) + `dimensions.name`
  distinguish/identify records. `dimensions.name` is the join key back to `machines` (`bmsTag`/`energyTag`).
- The **Avro topic schemas stay structured** (typed fields per device) for validation/evolution;
  PyFlink maps the decoded record into the `{time, value, dimensions, metadata}` shape at sink time.
- **dbt `staging/`** unpacks JSONB into typed columns (`value->>'CHWS_Temp'`::numeric, `dimensions->>'building'`)
  and joins to `dim_equipment` (from `machines`) to build the wide, typed facts below — flexible at
  the edge, strict at the marts.

## Kimball Dimensional Model (star schema)

**Conformed dimensions** (shared across facts):

- `dim_date` — calendar (day grain), generated via dbt.
- `dim_time` — time-of-day (minute grain) for 1-min telemetry.
- `dim_location` — building hierarchy from `building`/`buildingCode` (Building-Alpha/Beta/Gamma → BA/BB/BG), extensible to floor/zone.
- `dim_equipment` — **sourced directly from `machines.csv`** (the master dimension): surrogate key, `name`, `type` (Chiller | Air Compressor | Cooling Tower | AHU), `building`, and the **`bmsTag` / `energyTag` natural keys** used to join telemetry. `is_active` drives SCD/soft-delete. This is the conformed asset dimension joining BMS and EMS facts to one physical machine.
- `dim_machine` — **manufacturing machines** (EMS + MES): `machine_id`, `machine_type` (INJECTION_MOULDING | CNC | HEATING), building/floor, `energyTag` (joins EMS energy stream), rated power. The conformed asset for energy ↔ production analytics.
- `dim_product` — SKU, family, UoM (from ERP).
- `dim_work_order` — order no., product FK, machine FK, planned qty, due date, status (from ERP/MES).
- `dim_shift` — shift code, start/end, crew.

**Fact tables** (each grain stated explicitly):

- `fact_hvac_reading` — grain: 1 row per equipment per minute. Measures mirror the sample `value` keys per type:
  - AHU: `Supply_Temp`, `Return_Temp`, `Supply_RH`, `Return_RH`, optional `Supply_Flow`.
  - Chillers: `Chilled_Water_Supply_Temp`, `Chilled_Water_Return_Temp`, `Condensor_Supply_Temp`, `Condensor_Return_Temp` (+ derived ΔT / COP).
  - Chiller MainHeader: `CHWS_Temp`, `CHWR_Temp`, `CDWS_Temp`, `CDWR_Temp`, `CHW_DPT`, `CW_DPT`, flow.
  - Cooling Tower header / Air Compressors / Air Coolers: outdoor temp/RH, `Total_Flow`, supply temp.
  - (Modeled as a long/narrow fact `fact_hvac_reading(equipment_key, date_key, time_key, metric, value, unit)` OR wide-per-type facts — recommend **wide per-type facts** `fact_ahu_reading`, `fact_chiller_reading`, etc. for clean Metabase/Power BI semantics; all conformed on `dim_equipment`/date/time via `bmsTag`.)
- `fact_energy_reading` — grain: 1 row per **manufacturing machine** per minute (EMS). Measures: `totalPower`, `totalEnergy` (+ derived delta kWh), joined to `dim_machine` via `energyTag`. Machine energy profile tracks its production state (e.g. injection moulding heat/clamp cycles, CNC spindle load, heating ramp). (Extensible to V/A/PF/Hz.)
- `fact_production` — grain: 1 row per work order per machine per status-change (MES/ERP). Measures: good qty, scrap qty, planned vs actual duration, OEE components (availability, performance, quality).
- `fact_machine_state` — grain: 1 row per machine state interval (RUN/IDLE/DOWN) for downtime analysis.

**Cross-domain analytics enabled** (the payoff): energy kWh per produced unit, HVAC load vs. production schedule, chiller COP vs. ambient/production heat load, demand peaks by shift.

---

## Repository Structure

```
acn-development/
├── docker-compose.yml                # all services, profiles: edge, stream, warehouse, orchestrate, bi
├── .env.example
├── README.md
├── plan.md
├── samples/          # provided reference: bms.csv, ems.csv, machines.csv (drive sim calibration + dbt seeds)
├── common/           # shared lib: device registry, modbus codec, generators, modbus server, publisher, avro_io
├── simulators/
│   ├── bms/          # Modbus TCP slave entrypoint for HVAC assets
│   └── ems/          # Modbus TCP slave entrypoint for manufacturing machines
├── gateways/
│   ├── bms_gateway/  # per-device Modbus master poll @60s → decode → Avro → Kafka
│   └── ems_gateway/
├── erp/
│   ├── source_db/    # Postgres init: schema + seed (products, machines, work_orders, production_runs, ...)
│   ├── api/          # FastAPI MES/ERP service (production scheduling endpoints + writes to source DB)
│   └── debezium/     # connector configs (postgres CDC source + JDBC sink) + register.sh
├── kafka/            # create-topics.sh (per-domain topics keyed by deviceId)
├── connect/          # Kafka Connect image (Debezium + JDBC connector)
├── schemas/          # Avro .avsc (single generic Telemetry schema)
├── flink/            # PyFlink job: kafka(topic-pattern, Avro) → JDBC sink to raw.telemetry
├── warehouse/
│   ├── init/         # Postgres + TimescaleDB: extensions, schemas, raw.telemetry hypertable
│   └── dbt/          # dbt project: staging/ + marts/ (dims, facts) + schema.yml tests
├── airflow/
│   └── dags/         # dbt_build dag, erp_api_batch dag
├── metabase/         # Metabase config + dashboards on the star schema (marts)
└── grafana/          # provisioned datasources + real-time ops dashboards
```

---

## Workstreams ("agents") — clear instructions per component

Each workstream below is self-contained and can be built/owned independently. Build order
follows dependencies (platform → producers → processing → modeling → serving).

### Agent 1 — Kafka Platform & Schemas (foundation)

- Stand up Kafka (KRaft, single broker for demo), Schema Registry, Kafka Connect (hosts Debezium), Kafka UI.
- A single generic `Telemetry` Avro schema carries every BMS/EMS reading (typed envelope + `value` map<double>); registered automatically by the publishers.
- Create domain/type topics **partitioned & keyed by `deviceId`** so each machine is one logical stream (e.g. `bms.hvac.chiller`, `bms.hvac.ahu`, `ems.machine.injection_moulding`, `ems.machine.cnc`, `ems.machine.heating`). Naming convention `<domain>.<group>.<type>`; key = `deviceId`.
- **Deliverable:** healthy broker, registry reachable, topics + subjects created.

### Agent 2 — BMS HVAC IoT Simulators + Publishers (one stream per device)

- `simulators/bms`: each HVAC asset (every AHU, Chiller, Chiller header, Cooling Tower header, Air Compressor, Air Cooler from `machines.csv`/`bms.csv`, across Building-Alpha/Beta/Gamma) is its **own `pymodbus` TCP slave unit**. Generators reproduce the sample metric keys + realistic ranges (diurnal temp curves, setpoint tracking, occasional faults), calibrated to `bms.csv` distributions.
- `gateways/bms_gateway`: a publisher task **per device** (config-driven from the device registry) polling its own slave every 60s, decoding via the shared register codec, serializing to **Avro**, producing to the matching `bms.hvac.<type>` topic **keyed by `deviceId`**. Payload carries `building`, `equipmentType`, `name`/`deviceId`, and the metric `value` map — raw, per-device, un-aggregated.
- **Deliverable:** independent 1-min Avro streams per HVAC device on `bms.hvac.*`, each device on its own key/partition, verifiable in Kafka UI.

### Agent 3 — EMS Manufacturing-Machine IoT Simulators + Publishers (one stream per machine)

- `simulators/ems`: each **production machine** — Injection Moulding, CNC, Heating (multiple instances across buildings/floors) — is its **own Modbus TCP slave unit** exposing `totalEnergy` (cumulative) + `totalPower`, with **machine-type-specific load profiles** (injection moulding clamp/heat cycles, CNC spindle load steps, heating ramp/soak). Energy profiles correlate with each machine's production state so EMS ↔ MES joins are meaningful.
- `gateways/ems_gateway`: one publisher task **per machine** → `ems.machine.<type>` topics, Avro, **keyed by `deviceId`/`energyTag`**. Reuses the shared publisher to minimize divergence.
- **Deliverable:** independent 1-min Avro energy streams per manufacturing machine on `ems.machine.*`.

### Agent 4 — ERP/MES Source + API + Debezium CDC

- `erp/source_db`: Postgres source DB with logical replication enabled (`wal_level=logical`); schema for `products`, `machines` (Injection Moulding / CNC / Heating, conformed with EMS via `energyTag`), `work_orders`, `production_runs`, `machine_states`, `shifts`, plus seed data.
- `erp/api`: FastAPI service exposing MES/ERP production-scheduling endpoints (read schedule, create/update work orders, post production runs, machine states) — writes land in the source DB.
- `erp/debezium`: Debezium Postgres source (Avro, ExtractNewRecordState unwrap) emits `erp.public.*` topics; Confluent JDBC sink auto-creates structured tables in `erp_raw` (`?currentSchema=erp_raw`, RegexRouter strips the prefix).
- **Deliverable:** API mutations produce CDC events on `erp.*` topics and land in `erp_raw.*`.

### Agent 5 — PyFlink Stream Processing

- `flink/`: one PyFlink Table API job consuming all `bms.hvac.*` and `ems.machine.*` via a single `topic-pattern` Kafka source (shared Avro schema, Schema Registry). Python scalar UDFs reshape each record into the unified `{time, value, dimensions, metadata}` shape; JDBC sink writes JSON text that Postgres coerces to `jsonb` (`stringtype=unspecified`). Event-time watermarks declared for future windowed/derived features.
- Keep ERP CDC on a **Kafka Connect JDBC sink** straight into `erp_raw.*` (no Flink needed for slowly-changing reference data).
- **Deliverable:** unified RAW telemetry continuously landing in the `raw.telemetry` hypertable.

### Agent 6 — Warehouse (Postgres + TimescaleDB) + dbt Kimball Models

- `warehouse/init`: create extensions (`timescaledb`), schemas (`raw`, `staging`, `marts`, `erp_raw`), the unified **`raw.telemetry` hypertable** (`time`, `value` jsonb, `dimensions` jsonb, `metadata` jsonb + generated `device_id`/`source`) with GIN indexes + compression + retention policies. The Connect JDBC sink auto-creates the `erp_raw.*` tables.
- `warehouse/dbt`: dbt project.
  - `staging/`: `stg_*` views that **unpack `raw.telemetry` JSONB** (`value->>'CHWS_Temp'`::numeric, `dimensions->>'building'`, etc.) filtered by `dimensions->>'equipmentType'` / `source`, plus `erp_raw` cleanup (type casts, dedupe CDC to latest).
  - Seed `dim_equipment` (HVAC) + `dim_location` from `samples/machines.csv` (dbt seed). Build `dim_machine` (manufacturing: Injection Moulding/CNC/Heating) from the ERP `machines` table via CDC. Join telemetry via `bmsTag` (HVAC) / `energyTag` (machines).
  - `marts/`: build the dimensions and facts from the model above. SCD Type 2 on `dim_work_order` where status changes matter; Type 1/soft-delete (`is_active`) on `dim_equipment`.
  - Generate `dim_date` / `dim_time` via dbt.
  - **dbt tests** in `schema.yml`: `not_null`, `unique` on surrogate keys, `relationships` (fact→dim FKs), `accepted_values` (equipment_type, states). Document a "Future Work" note for Great Expectations at ingestion.
- **Deliverable:** queryable star schema in `marts`, `dbt test` green, `dbt docs` generated.

### Agent 7 — Airflow Orchestration

- `airflow/dags`: `dbt_build` DAG (staging → marts → test) on a schedule aligned to telemetry cadence; `erp_api_batch` DAG for any non-CDC API pulls (e.g. master data refresh). Use `BashOperator` for dbt.
- **Deliverable:** scheduled, observable pipeline runs in Airflow UI.

### Agent 8 — BI / Serving

- **Metabase** (`metabase/`, containerized): connect to the `marts` schema, model the star schema (table relationships + segments/metrics), and build dashboards (energy by building/equipment, chiller efficiency, kWh per produced unit, production vs. facility load). Self-serve question builder lets non-SQL users explore the dims/facts.
- **Grafana** (`grafana/`): provisioned TimescaleDB datasource + real-time ops dashboards (HVAC trends, energy demand, alarms) for sub-minute operational visibility off `raw.telemetry`.
- **Deliverable:** Metabase dashboards on the star schema + Grafana real-time ops dashboards, both provisioned in containers.

### Agent 9 — Compose Integration & Delivery

- `docker-compose.yml` with **profiles** (`edge`, `stream`, `warehouse`, `orchestrate`, `bi`), healthchecks, dependency ordering, and `.env`.
- `README.md`: one-command bring-up per profile, port map, troubleshooting, and a Future Work section (Great Expectations, medallion lakehouse w/ MinIO+Iceberg, Kafka Connect→Flink alternatives, Kubernetes/Strimzi migration, schema-evolution policy).
- **Deliverable:** `docker compose --profile <x> up` brings the whole stack online reproducibly.

---

## Build Phases (milestones)

1. **Platform up** — Kafka + Schema Registry + Connect + Postgres/Timescale + Airflow skeleton (Agents 1, 6-init).
2. **Producers** — BMS + EMS simulators/gateways streaming Avro; ERP source+API+Debezium CDC (Agents 2, 3, 4).
3. **Processing** — PyFlink job + Connect JDBC sink landing RAW (Agent 5).
4. **Modeling** — dbt staging + Kimball marts + tests, Airflow scheduling (Agents 6, 7).
5. **Serving** — Grafana dashboards + Metabase star-schema dashboards (Agent 8).
6. **Delivery** — Compose profiles, healthchecks, README, Future Work (Agent 9).

---

## Verification (end-to-end)

- **Producers (per-machine streams):** Kafka UI shows independent 1-min Avro streams on `bms.hvac.*` and `ems.machine.*`, each device on its own message key/partition; subjects in Schema Registry. `kafka-avro-console-consumer` decodes a sample and confirms `deviceId` keying.
- **CDC:** insert/update a work order via the FastAPI endpoint → corresponding `erp.public.work_orders` change event appears; row lands in `erp_raw.work_orders`.
- **Stream→RAW:** `SELECT count(*), max("time") FROM raw.telemetry` increases each minute; TimescaleDB `hypertable_size` confirms chunking.
- **Modeling:** `dbt build` then `dbt test` all pass; spot-check `fact_energy_reading` joins cleanly to `dim_machine`/`dim_date`/`dim_time`; referential tests green.
- **Cross-domain join (tag bridge):** confirm a single asset (e.g. `BA-CHILLER-01`) ties HVAC + energy: `dim_equipment` joins `fact_hvac_reading` via `bmsTag` (`BA-Chiller01`) and energy via `energyTag` — chiller ΔT vs. power returns sensible values.
- **Cross-domain query:** run a sample SQL computing **kWh per produced unit by shift** joining `fact_energy_reading` + `fact_production` + `dim_shift` — returns sensible values.
- **Serving:** Grafana ops dashboard renders live HVAC/energy trends; Metabase connects to `marts`, models star-schema relationships, and renders dashboards (energy by equipment, chiller efficiency).
- **Delivery:** from a clean checkout, `docker compose --profile edge --profile stream --profile warehouse --profile orchestrate --profile bi up -d` reaches healthy on all services.

---

## Future Work (documented, out of scope now)

- Great Expectations checks at ingestion (ELT contract enforcement).
- Medallion lakehouse (MinIO + Iceberg/Parquet bronze/silver) feeding Postgres gold marts.
- Metadata/lineage catalog (OpenMetadata/DataHub).
- Kubernetes deployment (Strimzi for Kafka, Flink Operator) with Helm.
- Schema-evolution governance + dead-letter queues for malformed telemetry.
- Per-device Kafka topics (vs. partitioned-by-device) for very large fleets.

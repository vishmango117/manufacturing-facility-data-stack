# Manufacturing Facility Data Platform — Development Summary

## Project Overview

A containerized, platform-agnostic data engineering platform that unifies four operational
data sources — **BMS** (building management / HVAC), **EMS** (energy monitoring for
manufacturing machines), **MES/ERP** (production scheduling) — into one Postgres warehouse
with a **Kimball star-schema** dimensional model.

The platform simulates realistic field devices over Modbus TCP, streams telemetry through
Kafka with Avro serialization, captures ERP changes via Debezium CDC, processes streams
with PyFlink, models the data with dbt, and serves analytics via Metabase and Grafana.
Everything runs on a single `docker compose` stack with modular profiles.

---

## Architecture

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
                                   │ (event-time window) │  enrich, map → Postgres
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

## Technology Stack

| Layer | Technology | Rationale |
|---|---|---|
| **Field Protocol** | Modbus TCP (`pymodbus`) | Ubiquitous in industrial HVAC/energy metering; deterministic register model |
| **Streaming** | Kafka (KRaft, no ZooKeeper) | Single process, simpler ops; partitioned/keyed by deviceId for ordering |
| **Serialization** | Avro + Schema Registry | Typed schema evolution; compact binary format; contract enforcement |
| **CDC** | Debezium (Postgres connector) | Native logical replication; low latency; handles schema changes |
| **Stream Processing** | PyFlink (Table API) | Event-time windowing; Java/Python bridge; exactly-once semantics |
| **Warehouse** | Postgres + TimescaleDB | Single binary for all layers; hypertables for time-series; JSONB flexibility |
| **Transform** | dbt (SQL-only) | Version-controlled transformations; tests as code; docs auto-generation |
| **Orchestration** | Airflow (BashOperator + PostgresOperator) | DAG visibility; retry/SLA; native Postgres operator |
| **BI** | Metabase + Grafana | Metabase for star-schema self-serve; Grafana for real-time ops |
| **Deployment** | Docker Compose with profiles | Modular bring-up; no Kubernetes required for dev/demo |

---

## Component Inventory

### Edge Layer (simulators + gateways)

| Component | Description | Files |
|---|---|---|
| **BMS Simulator** | 39 Modbus TCP slaves (AHU, Chiller, Cooling Tower, Air Compressor, Air Cooler) with realistic value generators calibrated from `samples/bms.csv` | `simulators/bms/server.py` |
| **EMS Simulator** | 18 Modbus TCP slaves (Injection Moulding, CNC, Heating machines across 3 buildings) with type-specific load profiles | `simulators/ems/server.py` |
| **BMS Gateway** | Per-device publisher: polls its slave @60s, decodes via register map, Avro-serializes, produces to `bms.hvac.*` topics keyed by deviceId | `gateways/bms_gateway/publish.py` |
| **EMS Gateway** | Same pattern as BMS gateway → `ems.machine.*` topics | `gateways/ems_gateway/publish.py` |
| **Device Registry** | Canonical source of truth: parses `bms.csv` for BMS devices, generates 18 EMS machines from fleet config. Provides unit-id assignment, topic mapping, metric calibration | `common/registry.py` |

**Total devices:** 39 BMS + 18 EMS = **57 independent IoT streams**

### Shared Library (`common/`)

| Module | Responsibility |
|---|---|
| `registry.py` | Device catalog, topic mapping, metric specs, building/energy tag bridge |
| `modbus_codec.py` | IEEE-754 float ↔ 2-register encoding/decoding |
| `generators.py` | BMS metric value generators (diurnal curves, setpoint tracking), EMS power profiles, `EnergyIntegrator` (kWh accumulator) |
| `modbus_server.py` | `_build_context`, `_compute_values`, `run` — reusable Modbus TCP server loop |
| `publisher.py` | Per-device polling loop with Kafka producer, `deviceId` keying, retry logic |
| `avro_io.py` | `TELEMETRY_SCHEMA` constant (single generic schema for all devices) |

### ERP/MES Layer

| Component | Description |
|---|---|
| **Source DB** | Postgres with logical replication enabled; schema: `products`, `machines`, `work_orders`, `production_runs`, `machine_states`, `shifts` + seed data |
| **FastAPI Service** | 7 endpoints: `/health`, `/machines`, `/schedule`, `POST /work_orders`, `PATCH /work_orders/{id}/status`, `POST /production_runs`, `POST /machine_states` |
| **Debezium CDC** | Postgres connector → `erp.public.*` change topics; JDBC sink → `erp_raw.*` tables |

### Streaming Layer

| Component | Description |
|---|---|
| **Kafka (KRaft)** | Single broker, 9 domain/type topics, partitioned by deviceId |
| **Schema Registry** | Avro schema subject: `telemetry-value` |
| **Kafka Connect** | Hosts Debezium source + JDBC sink connectors |
| **PyFlink Job** | Consumes Avro from Kafka topics → validates → maps to unified `{time, value, dimensions, metadata}` → JDBC sink to `raw.telemetry` |

### Warehouse Layer

| Component | Description |
|---|---|
| **TimescaleDB** | Hypertable `raw.telemetry` (time, value jsonb, dimensions jsonb, metadata jsonb) with GIN indexes, compression, retention policies |
| **dbt Project** | Staging views (JSONB unpack + CDC dedupe), 8 conformed dimensions, 4 fact tables, schema tests |

### Orchestration & BI

| Component | Description |
|---|---|
| **Airflow** | 2 DAGs: `dbt_build` (seed → run → test → docs), `erp_api_batch` (health check + dimension refresh) |
| **Metabase** | Connects to `marts` schema; star-schema modeling; self-serve questions |
| **Grafana** | Provisioned datasources (TimescaleDB raw + marts); 2 dashboards (HVAC real-time, EMS energy) |

---

## Design Decisions

### 1. One IoT Stream Per Machine

**Decision:** Each physical device is its own independent Modbus TCP slave and Kafka producer.
No upstream aggregation. Every message is keyed by `deviceId`.

**Why:**
- Mirrors real field IoT where each asset reports independently.
- Guarantees per-device event ordering (one partition per key).
- Consumers can fan out per device without filtering.
- New devices can be added without re-partitioning existing streams.

**Trade-off:** More processes/containers (57 total) vs. fewer aggregated streams.
Mitigated by Docker Compose profiles and resource limits in production.

### 2. Single Generic Avro Schema

**Decision:** All devices share one Avro schema (`Telemetry` record with `deviceId`, `source`,
`equipmentType`, `building`, `ts_utc`, `value` map<double>, `metadata` map<string,string>).

**Why:**
- No schema per device type — adding a new equipment type requires zero schema changes.
- Schema evolution is simple: add new fields to the value map, not new subjects.
- PyFlink maps the decoded record into the unified `{time, value, dimensions, metadata}` shape.

**Trade-off:** Loss of per-metric typing at the ingestion layer. Mitigated by dbt staging
views that cast `value->>'metric_key'` to `numeric` with explicit error handling.

### 3. Unified JSONB Landing Table

**Decision:** `raw.telemetry` uses a schema-on-read approach with 4 columns: `time`,
`value` (jsonb), `dimensions` (jsonb), `metadata` (jsonb).

**Why:**
- New device types/metrics need no DDL changes.
- GIN indexes on `dimensions` and `value` enable fast key lookups.
- dbt staging unpacks JSONB into typed columns only where needed.
- Matches the "flexible at the edge, strict at the marts" pattern.

**Trade-off:** Slightly slower queries vs. native typed columns. Acceptable because:
- Raw layer is append-only and rarely queried directly.
- dbt marts materialize as typed tables.
- TimescaleDB compression handles the bulk of raw data efficiently.

### 4. Topic Strategy: Domain/Type, Not Per-Device

**Decision:** Topics are grouped by domain and equipment type (e.g. `bms.hvac.chiller`,
`ems.machine.injection_moulding`), partitioned and keyed by `deviceId`.

**Why:**
- 9 physical topics vs. 57 per-device topics — manageable Kafka cluster size.
- Equipment-type grouping enables type-level analytics without cross-topic joins.
- Per-device keying preserves ordering within each type topic.

**Trade-off:** Multiple device types share a topic. Mitigated by the `deviceId` key and
`dimensions.equipmentType` field for filtering. Documented as an alternative (per-device
topics) in Future Work.

### 5. Tag Bridge for Cross-Domain Analytics

**Decision:** `machines.csv` provides `bmsTag` (joins BMS device names) and `energyTag`
(joins EMS device names) as a conformed dimension bridge.

**Why:**
- A single physical asset (e.g. `BA-CHILLER-01`) ties its HVAC telemetry to its energy
  consumption via `dim_equipment` (HVAC side, `bmsTag`) and `dim_machine` (energy side,
  `energyTag`).
- Enables cross-domain queries: energy kWh per produced unit, chiller COP vs. production
  heat load, demand peaks by shift.

**Trade-off:** Requires `machines.csv` to be the authoritative master. If BMS and EMS
device names diverge in production, the bridge breaks. Mitigated by the device registry
enforcing name consistency at simulation time.

### 6. TimescaleDB Over Separate Time-Series DB

**Decision:** Use TimescaleDB (Postgres extension) instead of a dedicated time-series
database (InfluxDB, Timescale Cloud, etc.).

**Why:**
- Single binary: raw telemetry + ERP CDC + Kimball marts all in one database.
- SQL everywhere: dbt, Airflow, Metabase, Grafana all speak SQL.
- Hypertables provide time-series optimizations (chunking, compression, retention)
  within the Postgres ecosystem.
- No additional infrastructure to manage.

**Trade-off:** Less specialized than InfluxDB for extreme-scale time-series. Acceptable
for the simulation/demo scale (57 devices × 1-min cadence ≈ 82K points/day).

### 7. dbt Over Spark/Pandas for Transform

**Decision:** Use dbt (SQL-only) instead of Spark, Pandas, or custom ETL scripts.

**Why:**
- Transform logic lives in SQL files — version-controlled, tested, documented.
- `dbt test` provides not_null, unique, relationships, accepted_values out of the box.
- `dbt docs generate` produces auto-generated data dictionary.
- No Python dependency management for transform code.
- Airflow schedules dbt runs — the orchestrator stays thin.

**Trade-off:** Limited to SQL transformations. Complex data quality checks or ML features
would require Great Expectations or a separate pipeline. Documented in Future Work.

---

## Data Model

### Unified RAW Schema (TimescaleDB Hypertable)

| Column | Type | Description |
|---|---|---|
| `time` | `timestamptz` | Event time (UTC), hypertable partition key |
| `value` | `jsonb` | Meter/sensor readings (device-specific keys) |
| `dimensions` | `jsonb` | `building`, `equipmentType`, `name` |
| `metadata` | `jsonb` | `msgId`, `deviceId`, `source`, `topic`, `schema_ver` |

GIN indexes on `dimensions` and `value`; Timescale compression on chunks older than 7 days;
retention policy drops data older than 90 days.

### Kimball Star Schema

**8 Conformed Dimensions:**
- `dim_date` — calendar (day grain), generated via dbt
- `dim_time` — time-of-day (minute grain)
- `dim_location` — building hierarchy (BA/BB/BG)
- `dim_equipment` — HVAC equipment from `machines.csv` seed (bmsTag/energyTag bridge)
- `dim_machine` — manufacturing machines from ERP CDC
- `dim_product` — SKU, family, UoM from ERP CDC
- `dim_work_order` — order no., product FK, machine FK, planned qty, due date
- `dim_shift` — shift code, start/end, crew from ERP CDC

**4 Fact Tables:**
- `fact_hvac_reading` — grain: 1 row per HVAC equipment per minute (AHU, Chiller)
- `fact_energy_reading` — grain: 1 row per manufacturing machine per minute
- `fact_production` — grain: 1 row per work order per machine per status-change
- `fact_machine_state` — grain: 1 row per machine state interval (RUN/IDLE/DOWN)

**Cross-domain join path:**
```
fact_hvac_reading → dim_equipment.bmsTag → dim_equipment.energyTag → dim_machine → fact_energy_reading
fact_energy_reading → dim_machine → fact_production → dim_work_order
```

---

## Build Process & Lessons Learned

### What Worked Well

1. **Device registry as single source of truth** — `common/registry.py` drives simulator
   calibration, gateway polling, topic mapping, and dbt seeding. One file, consistent
   across the entire stack.

2. **Per-device Modbus slaves** — Each HVAC asset and manufacturing machine runs as its own
   `pymodbus` TCP slave with realistic value generators (diurnal curves, setpoint tracking,
   occasional faults). This mirrors real field IoT more accurately than a single aggregated
   simulator.

3. **Sample-driven calibration** — BMS metric ranges are derived from `samples/bms.csv`
   (min/max/nominal per device per metric). EMS power envelopes are defined per machine type.
   No hardcoded constants.

4. **dbt staging as JSONB unpack layer** — The generic `raw.telemetry` shape is flexible at
   ingestion; dbt staging views cast to typed columns only where needed. Equipment-type-specific
   views (`stg_telemetry_ahu`, `stg_telemetry_chiller`) compute derived metrics (delta temp,
   COP proxies) inline.

5. **Docker Compose profiles** — Modular bring-up: `edge`, `stream`, `warehouse`,
   `orchestrate`, `bi`. Each profile is independently testable.

### Challenges & Fixes

1. **Kafka cluster ID format** — Confluent KRaft requires a base64-encoded 16-byte UUID,
   not a standard UUID string. Fixed by generating with Python's `base64.urlsafe_b64encode`.

2. **Flink Docker image** — The official `flink:1.18.1` image is Java-only (no Python).
   `apache-flink` Python package requires JDK headers for `pemja` (native bridge).
   Solution: build from `python:3.10-slim`, download Flink binary distribution, install
   OpenJDK 17 for headers.

3. **YAML anchor compatibility** — Docker Compose v2.39.2 (via Podman) had issues with
   `<<: *common` anchors in some contexts. Worked around by using `FLINK_PROPERTIES` with
   literal block scalar (`|`) instead of list format.

4. **SAMPLES_DIR resolution** — The device registry needs `samples/bms.csv` at runtime.
   Local development (`/samples` doesn't exist) vs. container (`/samples` is mounted)
   required fallback logic: env var → `/samples` → walk up from `__file__`.

5. **Podman vs Docker** — Podman socket is available but `docker compose` binary had
   YAML anchor parsing issues. Used `podman run` directly for infrastructure services
   and built custom images with `podman build --format docker`.

### Validation Performed

- **Device registry:** 39 BMS devices parsed from samples, 18 EMS machines generated,
  all 9 topics correct.
- **Modbus codec:** 7/7 float round-trips pass with <1e-3 precision.
- **Generators:** BMS metric values produce realistic ranges within calibrated envelopes;
  EMS power profiles generate type-specific patterns; EnergyIntegrator correctly accumulates kWh.
- **Avro schema:** Parse/write/read round-trip successful.
- **YAML/JSON:** All compose, dbt, and connector configs parse without errors.
- **Python syntax:** All source files pass `ast.parse()` validation.
- **dbt dependency graph:** All model refs resolve; seed matches `ref('machines')`.
- **FastAPI:** All 7 endpoints verified.
- **Airflow DAGs:** Both parse with correct task definitions.

---

## Trade-Offs Summary

| Decision | Alternative Considered | Chosen | Rationale |
|---|---|---|---|
| Kafka KRaft vs ZooKeeper | ZooKeeper mode | KRaft | Single process, simpler ops |
| Avro vs JSON | JSON, Protobuf, Parquet | Avro | Schema Registry integration, typed validation |
| Single Avro schema vs per-type | Per-device-type schemas | Single generic | Zero schema changes for new equipment types |
| JSONB raw table vs typed columns | Native typed columns per metric | JSONB + dbt unpack | Flexible ingestion, strict marts |
| Domain/type topics vs per-device | 57 per-device topics | 9 domain/type topics | Manageable cluster size, keyed by deviceId |
| TimescaleDB vs InfluxDB | InfluxDB, QuestDB | TimescaleDB | Single binary, SQL everywhere |
| dbt vs Spark/Pandas | Spark, Pandas ETL | dbt (SQL) | Version-controlled, tested, documented |
| Airflow vs cron | Cron, GitHub Actions | Airflow | DAG visibility, retry/SLA, native operators |
| Metabase + Grafana | Power BI, Tableau | Metabase + Grafana | Open-source, containerized, complementary |
| Docker Compose vs Kubernetes | K8s, Strimzi, Helm | Docker Compose | Dev/demo simplicity, reproducible |

---

## Future Work

### Near-Term (1-3 months)

1. **Great Expectations at ingestion** — Contract enforcement on Avro records before they
   enter Kafka. Define suites per device type (required fields, value ranges, freshness).
   Would catch malformed data at the edge instead of downstream in dbt.

2. **Schema Registry evolution** — Register per-device-type Avro schemas alongside the
   generic schema. Use Schema Registry's backward-compatibility policy to govern field
   additions. Add a dead-letter queue for records that fail schema validation.

3. **Flink 1-min tumbling windows** — Compute per-device rolling averages, min/max, and
   kWh deltas in Flink before sinking to Postgres. Reduces raw telemetry volume by 60x
   (1-min → 1-hour aggregates).

4. **Per-device Kafka topics** — Alternative to the domain/type strategy. Each device gets
   its own topic with its own Avro schema subject. Better for large-scale deployments where
   device types are heterogeneous.

### Medium-Term (3-6 months)

5. **Medallion lakehouse** — MinIO + Iceberg/Parquet for bronze (raw) and silver (cleaned)
   layers. Postgres marts become the "gold" layer. Enables historical replay and point-in-time
   queries at scale.

6. **Power BI integration** — Native Postgres connector to the `marts` schema. Build the
   cross-domain dashboards: energy kWh per produced unit, chiller COP vs. production heat
   load, demand peaks by shift, OEE vs. facility load.

7. **Metadata catalog** — OpenMetadata or DataHub for lineage tracking (Kafka → Flink →
   Postgres → dbt → Metabase). Automatic data quality dashboards.

8. **Extended EMS metrics** — Add voltage (V), current (A), power factor (PF), and
   frequency (Hz) to EMS telemetry for power quality analysis and demand charge optimization.

### Long-Term (6-12 months)

9. **Kubernetes deployment** — Strimzi for Kafka, Flink Operator for stream processing,
   Helm charts for orchestration. Horizontal scaling for device counts >1000.

10. **Real-time anomaly detection** — Flink CEP (Complex Event Processing) for chiller
    ΔT anomalies, energy spike detection, machine state transition validation. Alert via
    Slack/PagerDuty webhook.

11. **SCD Type 2 for dim_equipment** — Track HVAC equipment lifecycle (install, decommission,
    retrofit) with proper slowly-changing dimension handling.

12. **Multi-tenant support** — Isolate BMS/EMS streams by facility/building. Add `tenantId`
    to metadata. Enable the platform to serve multiple manufacturing sites.

---

## Running the Platform

### With Docker Desktop

```bash
cp .env.example .env
docker compose --profile warehouse --profile stream --profile edge --profile orchestrate --profile bi up -d
```

### With Podman

```bash
cp .env.example .env
# Build custom images
podman build --format docker -f connect/Dockerfile -t acn/connect:latest .
podman build --format docker -f gateways/bms_gateway/Dockerfile -t acn/bms-gateway:latest .
podman build --format docker -f gateways/ems_gateway/Dockerfile -t acn/ems-gateway:latest .
podman build --format docker -f simulators/bms/Dockerfile -t acn/bms-simulator:latest .
podman build --format docker -f simulators/ems/Dockerfile -t acn/ems-simulator:latest .
podman build --format docker -f flink/Dockerfile -t acn/flink:1.18.1 .

# Create network
podman network create acn-network

# Start services individually (see docker-compose.yml for dependencies)
# Or use podman-compose:
pip install podman-compose
podman-compose --file docker-compose.yml --profile warehouse up -d
```

### Verification Queries

```sql
-- Telemetry volume
SELECT count(*), max("time") FROM raw.telemetry;

-- Cross-domain: chiller ΔT vs. power
SELECT
  e.name,
  avg(h.delta_temp) as avg_chiller_delta_temp,
  avg(e.total_power) as avg_power_kw
FROM marts.fact_hvac_reading h
JOIN marts.dim_equipment e ON h.equipment_key = e.equipment_key
JOIN marts.fact_energy_reading fe ON e.energy_tag = fe.energy_tag
WHERE e.equipment_type = 'Chillers'
GROUP BY e.name
ORDER BY avg_power_kw DESC
LIMIT 10;

-- Energy per produced unit by shift
SELECT
  s.shift_code,
  sum(fe.total_energy_delta_kwh) as total_kwh,
  sum(p.good_qty) as total_units,
  sum(fe.total_energy_delta_kwh) / nullif(sum(p.good_qty), 0) as kwh_per_unit
FROM marts.fact_energy_reading fe
JOIN marts.dim_machine m ON fe.machine_key = m.machine_key
JOIN marts.fact_production p ON m.machine_key = p.machine_key
JOIN marts.dim_shift s ON p.shift_key = s.shift_key
GROUP BY s.shift_code
ORDER BY kwh_per_unit DESC;
```

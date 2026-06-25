# Manufacturing Facility Data Platform

A containerized, platform-agnostic data engineering platform that unifies four operational
data sources — **BMS** (HVAC facility data), **EMS** (energy data from manufacturing machines),
**MES/ERP** (production/scheduling) — into one Postgres warehouse modeled with **Kimball
star-schema dimensional modeling**.

[**📖 Development Summary**](DEVELOPMENT.md) — architecture decisions, trade-offs,
component inventory, data model, lessons learned, and future work.

[**📋 Build Plan**](plan.md) — original project plan with workstreams and milestones.

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

## Technology Stack

| Concern                  | Choice                                                                            |
| ------------------------ | --------------------------------------------------------------------------------- |
| Field protocol (BMS/EMS) | Modbus TCP, simulated, 1-min interval (`pymodbus`)                                |
| Streaming backbone       | Apache Kafka (KRaft mode, no ZooKeeper)                                           |
| Serialization            | **Avro + Confluent Schema Registry**                                              |
| ERP/MES ingest           | Postgres source DB + REST API; **Debezium** CDC → Kafka                           |
| Stream processing        | **PyFlink** (event-time windowing) → Postgres                                     |
| Warehouse                | **Postgres + TimescaleDB** (hypertables for raw telemetry, star schema for marts) |
| Transform / modeling     | **dbt** (Kimball dims + facts)                                                    |
| Orchestration            | **Apache Airflow** (dbt runs + API batch pulls)                                   |
| Data quality             | **dbt tests** (not_null/unique/relationships/accepted_values)                     |
| BI / serving             | **Metabase** (native Postgres connector) + **Grafana** (real-time ops)            |
| Deployment               | **Docker Compose** with profiles                                                  |

## Quick Start

### Prerequisites

- **Docker Desktop** (or Docker + Compose plugin), **or**
- **Podman** 5.x + `podman-compose` (Python package)
- 8 GB RAM minimum (16 GB recommended)

> **Note:** The platform has been validated with both Docker Desktop and Podman.
> Podman users should build custom images with `--format docker` (Podman's OCI format
> drops HEALTHCHECK instructions, which Compose healthchecks rely on).

### Bring up the platform

```bash
# Copy environment
cp .env.example .env

# 1. Warehouse (Postgres + TimescaleDB)
docker compose --profile warehouse up -d

# 2. Streaming backbone (Kafka + Schema Registry + Connect + Flink)
docker compose --profile stream up -d

# 3. Edge simulators + gateways (BMS/EMS field data)
docker compose --profile edge up -d

# 4. Orchestration (Airflow + dbt)
docker compose --profile orchestrate up -d

# 5. BI / serving (Metabase + Grafana)
docker compose --profile bi up -d
```

Or bring everything up at once:

```bash
docker compose --profile warehouse --profile stream --profile edge --profile orchestrate --profile bi up -d
```

### Service Ports

| Service              | Port | URL                                       |
| -------------------- | ---- | ----------------------------------------- |
| Metabase             | 3000 | http://localhost:3000                     |
| Grafana              | 3001 | http://localhost:3001 (admin/admin)       |
| Kafka UI             | 8090 | http://localhost:8090                     |
| Flink UI             | 8081 | http://localhost:8081                     |
| ERP API              | 8000 | http://localhost:8000                     |
| Airflow              | 8082 | http://localhost:8082 (admin/admin)       |
| Warehouse (Postgres) | 5432 | psql -h localhost -U facility -d facility |

## Repository Structure

```
acn-development/
├── docker-compose.yml                # all services, profiles: edge, stream, warehouse, orchestrate, bi
├── .env.example
├── README.md
├── samples/          # reference data: bms.csv, ems.csv, machines.csv
├── common/           # shared library: registry, codec, generators, modbus, publisher, avro_io
├── simulators/
│   ├── bms/          # Modbus TCP slave: HVAC register maps + realistic value generators
│   ├── ems/          # Modbus TCP slave: energy meter register maps
│   └── shared/       # register-map definitions, value-profile helpers
├── gateways/
│   ├── bms_gateway/  # Modbus master poll @60s → decode → Avro → Kafka
│   └── ems_gateway/
├── erp/
│   ├── source_db/    # Postgres init: schema + seed (products, work_orders, production_runs)
│   ├── api/          # FastAPI MES/ERP service (production scheduling endpoints)
│   └── debezium/     # connector config (postgres CDC → erp.* topics)
├── schemas/          # Avro .avsc for telemetry (single generic schema)
├── flink/            # PyFlink jobs: kafka→validate→window→JDBC sink to Postgres
├── warehouse/
│   ├── init/         # Postgres + TimescaleDB: create_extensions, raw hypertables
│   └── dbt/          # dbt project: staging/ + marts/ (dims, facts) + schema.yml tests
├── airflow/
│   └── dags/         # dbt_run dag, erp_api_batch dag
├── metabase/         # Metabase config + dashboards on the star schema (marts)
└── grafana/          # provisioned datasources + real-time ops dashboards
```

## Design Principles

### One IoT Stream Per Machine

**Every machine is its own independent stream/publisher.** Each device runs as its own
Modbus TCP slave and has its own publisher process that emits only that device's telemetry —
mirroring real field IoT where each asset reports independently. No upstream aggregation.

- Every message carries `deviceId` (and `source`), and is **produced keyed by `deviceId`** so a
  given machine's events stay ordered on one partition and consumers can fan out per device.
- **Topic strategy:** group by domain/equipment-type topics **partitioned/keyed by device**
  (e.g. `bms.hvac.chiller`, `ems.machine.injection_moulding`) rather than one giant topic.
- **Raw IoT architecture** — telemetry is not pre-aggregated at the edge.

### Tag Bridge for Cross-Domain Analytics

The `machines.csv` `bmsTag` joins to BMS `name` and `energyTag` joins to EMS `name` — this
bridge is what lets a single physical asset (e.g. `BA-CHILLER-01`) tie its HVAC telemetry
to its energy consumption, enabling:

- Energy kWh per produced unit
- HVAC load vs. production schedule
- Chiller COP vs. production heat load
- Demand peaks by shift

### Unified JSONB Landing

The `raw.telemetry` hypertable uses a schema-on-read approach with 4 columns:

| Column       | Type          | Description                                |
| ------------ | ------------- | ------------------------------------------ |
| `time`       | `timestamptz` | Event time (UTC)                           |
| `value`      | `jsonb`       | Meter/sensor readings                      |
| `dimensions` | `jsonb`       | Building, equipment type, device name      |
| `metadata`   | `jsonb`       | msgId, deviceId, source, topic, schema_ver |

dbt staging unpacks JSONB into typed columns for the strict Kimball facts downstream.

## Kimball Star Schema

### Conformed Dimensions

- `dim_date` — calendar (day grain)
- `dim_time` — time-of-day (minute grain)
- `dim_location` — building hierarchy (BA/BB/BG)
- `dim_equipment` — HVAC equipment from `machines.csv` (bmsTag/energyTag bridge)
- `dim_machine` — manufacturing machines (INJECTION_MOULDING/CNC/HEATING)
- `dim_product` — SKU, family, UoM
- `dim_work_order` — order no., product FK, machine FK
- `dim_shift` — shift code, start/end, crew

### Fact Tables

- `fact_hvac_reading` — 1 row per HVAC equipment per minute (AHU, Chiller, Cooling Tower, etc.)
- `fact_energy_reading` — 1 row per manufacturing machine per minute (totalPower, totalEnergy)
- `fact_production` — 1 row per work order per machine per status-change
- `fact_machine_state` — 1 row per machine state interval (RUN/IDLE/DOWN)

## Verification

### End-to-end checks

```bash
# 1. Check all services are healthy
docker compose ps

# 2. Verify Kafka topics
docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --list

# 3. Check telemetry landing in Postgres
docker compose exec warehouse psql -U facility -d facility -c \
  "SELECT count(*), max(\"time\") FROM raw.telemetry;"

# 4. Run dbt tests
docker compose exec airflow-scheduler dbt test --profiles-dir /opt/airflow/dbt --profile acn_platform

# 5. Cross-domain join: chiller ΔT vs. totalPower
docker compose exec warehouse psql -U facility -d facility -c "
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
"
```

## Future Work

- **Great Expectations** checks at ingestion (ELT contract enforcement).
- **Medallion lakehouse** (MinIO + Iceberg/Parquet bronze/silver) feeding Postgres gold marts.
- **Metadata/lineage catalog** (OpenMetadata/DataHub).
- **Kubernetes deployment** (Strimzi for Kafka, Flink Operator) with Helm.
- **Schema-evolution governance** + dead-letter queues for malformed telemetry.
- **Per-device Kafka topics** as an alternative to per-domain/type topic strategy.
- **Extended EMS metrics** (V/A/PF/Hz) for power quality analysis.
- **Automated Testing suite** (Unit tests for edge modules, end-to-end integration tests for the data pipeline).
- **Extensive Documentation** (Full API specifications, interactive data lineage diagrams, and operational runbooks).
- **CI/CD Pipelines via GitHub Actions** (Automated linting, testing, and container image build/push workflows).

## Troubleshooting

### Kafka not starting

```bash
docker compose logs kafka
docker compose logs kafka-controller
```

### Debezium connector failing to register

```bash
# Check Kafka Connect is healthy
curl http://localhost:8083/

# Re-register connectors
cd erp/debezium && CONNECT_URL=http://localhost:8083 sh register.sh
```

### Flink job failing

```bash
# Check Flink jobmanager logs
docker compose logs flink-jobmanager

# Verify Schema Registry connectivity
docker compose exec flink-jobmanager curl -s http://schema-registry:8081/subjects
```

### dbt build failing

```bash
# Check warehouse is healthy first
docker compose --profile warehouse up -d

# Run dbt with verbose logging
docker compose exec airflow-scheduler dbt run --profiles-dir /opt/airflow/dbt --profile acn_platform --log-format json
```

### Port conflicts

Edit `.env` to change port mappings in `docker-compose.yml` or stop conflicting services on the host.

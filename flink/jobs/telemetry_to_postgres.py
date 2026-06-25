"""PyFlink stream job: Kafka (Avro/Schema Registry) -> raw.telemetry (Postgres).

All BMS/EMS streams share one generic ``Telemetry`` Avro schema, so a single
``topic-pattern`` Kafka source consumes every per-device stream. Python scalar
UDFs reshape each record into the unified ``{time, value, dimensions, metadata}``
landing shape; the JDBC sink writes JSON text that Postgres coerces to ``jsonb``
(connection ``stringtype=unspecified``).

The job lands telemetry 1:1 (raw, un-aggregated). Event-time watermarks are
declared so windowed/derived features can be added later without re-plumbing.
"""

from __future__ import annotations

import json
import os

from pyflink.common import Configuration
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.types import DataTypes
from pyflink.table.udf import udf

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
SCHEMA_REGISTRY = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
PG_URL = os.environ.get(
    "PG_JDBC_URL",
    "jdbc:postgresql://warehouse:5432/facility?stringtype=unspecified",
)
PG_USER = os.environ.get("PG_USER", "facility")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "facility")
TOPIC_PATTERN = os.environ.get("TOPIC_PATTERN", "(bms|ems)\\..*")


@udf(result_type=DataTypes.STRING())
def map_to_json(m) -> str:
    return json.dumps({k: float(v) for k, v in (m or {}).items()})


@udf(result_type=DataTypes.STRING())
def meta_to_json(m) -> str:
    return json.dumps({k: str(v) for k, v in (m or {}).items()})


@udf(result_type=DataTypes.STRING())
def build_dims(building: str, equipment_type: str, device_id: str) -> str:
    return json.dumps({
        "building": building,
        "equipmentType": equipment_type,
        "name": device_id,
    })


def main() -> None:
    config = Configuration()
    config.set_string("parallelism.default", os.environ.get("FLINK_PARALLELISM", "2"))
    env = TableEnvironment.create(
        EnvironmentSettings.new_instance().in_streaming_mode().with_configuration(config).build()
    )

    env.create_temporary_function("map_to_json", map_to_json)
    env.create_temporary_function("meta_to_json", meta_to_json)
    env.create_temporary_function("build_dims", build_dims)

    env.execute_sql(f"""
        CREATE TABLE telemetry_source (
            deviceId       STRING,
            `source`       STRING,
            equipmentType  STRING,
            building       STRING,
            ts_utc         TIMESTAMP(3),
            `value`        MAP<STRING, DOUBLE>,
            metadata       MAP<STRING, STRING>,
            WATERMARK FOR ts_utc AS ts_utc - INTERVAL '30' SECOND
        ) WITH (
            'connector'                  = 'kafka',
            'topic-pattern'              = '{TOPIC_PATTERN}',
            'properties.bootstrap.servers' = '{BOOTSTRAP}',
            'properties.group.id'        = 'flink-telemetry',
            'scan.startup.mode'          = 'latest-offset',
            'format'                     = 'avro-confluent',
            'avro-confluent.url'         = '{SCHEMA_REGISTRY}'
        )
    """)

    env.execute_sql(f"""
        CREATE TABLE telemetry_sink (
            `time`      TIMESTAMP(3),
            `value`     STRING,
            dimensions  STRING,
            metadata    STRING
        ) WITH (
            'connector'                 = 'jdbc',
            'url'                       = '{PG_URL}',
            'table-name'                = 'raw.telemetry',
            'username'                  = '{PG_USER}',
            'password'                  = '{PG_PASSWORD}',
            'sink.buffer-flush.max-rows'= '200',
            'sink.buffer-flush.interval'= '2s'
        )
    """)

    env.execute_sql("""
        INSERT INTO telemetry_sink
        SELECT
            ts_utc                                       AS `time`,
            map_to_json(`value`)                         AS `value`,
            build_dims(building, equipmentType, deviceId) AS dimensions,
            meta_to_json(metadata)                       AS metadata
        FROM telemetry_source
    """).wait()


if __name__ == "__main__":
    main()

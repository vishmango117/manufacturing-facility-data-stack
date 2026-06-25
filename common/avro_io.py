"""Avro + Schema Registry helpers for the telemetry publishers.

A single, generic Avro schema (``Telemetry``) carries every BMS/EMS reading. The
device-specific metric map lives in the ``value`` field (Avro map<double>) so new
device types need no schema change — schema evolution stays additive, while the
envelope (deviceId, source, ts, building, equipmentType, topic) is strongly typed.
"""

from __future__ import annotations

import os
from typing import Dict

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# Generic telemetry envelope — matches the unified raw.telemetry landing shape.
TELEMETRY_SCHEMA = """
{
  "type": "record",
  "name": "Telemetry",
  "namespace": "facility.iot",
  "fields": [
    {"name": "deviceId",      "type": "string"},
    {"name": "source",        "type": "string"},
    {"name": "equipmentType", "type": "string"},
    {"name": "building",      "type": "string"},
    {"name": "ts_utc",        "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "value",         "type": {"type": "map", "values": "double"}},
    {"name": "metadata",      "type": {"type": "map", "values": "string"}}
  ]
}
"""


def schema_registry_client() -> SchemaRegistryClient:
    url = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    return SchemaRegistryClient({"url": url})


def telemetry_serializer(client: SchemaRegistryClient | None = None) -> AvroSerializer:
    client = client or schema_registry_client()
    return AvroSerializer(
        schema_registry_client=client,
        schema_str=TELEMETRY_SCHEMA,
        conf={"auto.register.schemas": True},
    )


def to_dict(obj: Dict, ctx) -> Dict:  # AvroSerializer to_dict callback
    return obj

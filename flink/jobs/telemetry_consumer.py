"""Lightweight Kafka → raw.telemetry consumer (replaces PyFlink on arm64).

Subscribes to all bms.* and ems.* topics, deserialises Avro via Schema Registry,
and upserts each record into the raw.telemetry hypertable with the same
{time, value, dimensions, metadata} shape that the PyFlink job produces.
"""

from __future__ import annotations

import json
import logging
import os
import re
import signal
import sys
import time

import psycopg2
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

log = logging.getLogger("telemetry_consumer")

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
PG_DSN = os.environ.get(
    "PG_DSN",
    "host=warehouse port=5432 dbname=facility user=facility password=facility",
)
GROUP_ID = os.environ.get("CONSUMER_GROUP", "telemetry-consumer")
TOPIC_PATTERN = re.compile(os.environ.get("TOPIC_PATTERN_RE", r"(bms|ems)\..*"))

INSERT_SQL = """
INSERT INTO raw.telemetry (time, value, dimensions, metadata)
VALUES (%s, %s, %s, %s)
"""

_running = True


def _signal_handler(sig, frame):
    global _running
    log.info("Shutdown signal received, draining…")
    _running = False


def _connect_pg(dsn: str):
    while True:
        try:
            conn = psycopg2.connect(dsn)
            log.info("Connected to Postgres")
            return conn
        except Exception as exc:
            log.warning("Postgres not ready yet: %s — retrying in 5s", exc)
            time.sleep(5)


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    sr = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    deserializer = AvroDeserializer(sr)

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })

    # List all topics and subscribe to matching ones.
    meta = consumer.list_topics(timeout=30)
    topics = [t for t in meta.topics if TOPIC_PATTERN.match(t)]
    if not topics:
        log.error("No matching topics found for pattern %s — waiting", TOPIC_PATTERN.pattern)
    log.info("Subscribing to %d topics: %s", len(topics), topics)
    consumer.subscribe(topics)

    pg = _connect_pg(PG_DSN)
    cur = pg.cursor()

    batch: list[tuple] = []
    BATCH_SIZE = 50
    FLUSH_INTERVAL = 2.0
    last_flush = time.monotonic()

    def flush():
        nonlocal batch
        if not batch:
            return
        try:
            cur.executemany(INSERT_SQL, batch)
            pg.commit()
            log.info("Inserted %d rows into raw.telemetry", len(batch))
        except Exception as exc:
            log.error("DB write failed: %s", exc)
            pg.rollback()
        batch = []

    while _running:
        msg = consumer.poll(timeout=0.5)
        if msg is None:
            if time.monotonic() - last_flush >= FLUSH_INTERVAL:
                flush()
                last_flush = time.monotonic()
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            log.error("Consumer error: %s", msg.error())
            continue

        try:
            record = deserializer(
                msg.value(),
                SerializationContext(msg.topic(), MessageField.VALUE),
            )
            if record is None:
                continue

            ts_raw = record.get("ts_utc")
            from datetime import datetime, timezone
            if isinstance(ts_raw, datetime):
                ts = ts_raw.replace(tzinfo=None)
            else:
                ts = datetime.fromtimestamp(ts_raw / 1000.0, tz=timezone.utc).replace(tzinfo=None)
            value_json = json.dumps({k: float(v) for k, v in (record.get("value") or {}).items()})
            dims_json = json.dumps({
                "building": record.get("building", ""),
                "equipmentType": record.get("equipmentType", ""),
                "name": record.get("deviceId", ""),
            })
            meta_json = json.dumps({k: str(v) for k, v in (record.get("metadata") or {}).items()})

            batch.append((ts, value_json, dims_json, meta_json))

            if len(batch) >= BATCH_SIZE or (time.monotonic() - last_flush >= FLUSH_INTERVAL):
                flush()
                last_flush = time.monotonic()

        except Exception as exc:
            log.warning("Failed to process message from %s: %s", msg.topic(), exc)

    flush()
    consumer.close()
    cur.close()
    pg.close()
    log.info("Consumer stopped cleanly")


if __name__ == "__main__":
    main()

"""Reusable Modbus master -> Avro -> Kafka publisher.

Honours the **one stream per machine** principle: for every device in the registry
an independent asyncio task polls *only that device's* registers over Modbus and
produces *its own* Avro message, keyed by ``deviceId``, to the device's topic. No
cross-device aggregation happens here.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid

from confluent_kafka import Producer
from confluent_kafka.serialization import MessageField, SerializationContext
from pymodbus.client import AsyncModbusTcpClient

from .avro_io import telemetry_serializer
from .modbus_codec import decode_metrics
from .registry import Device, devices_for

log = logging.getLogger("publisher")


def _producer() -> Producer:
    return Producer({
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"),
        "client.id": os.environ.get("CLIENT_ID", "iot-publisher"),
        "linger.ms": 50,
        "compression.type": "snappy",
    })


async def _poll_device(client: AsyncModbusTcpClient, device: Device,
                       producer: Producer, serializer, interval: float) -> None:
    """One independent IoT stream: poll -> decode -> Avro -> produce (keyed)."""
    n_registers = len(device.metrics) * 2
    while True:
        started = time.time()
        try:
            rr = await client.read_holding_registers(
                address=0, count=n_registers, slave=device.unit_id)
            if rr.isError():
                raise RuntimeError(str(rr))
            floats = decode_metrics(rr.registers, len(device.metrics))
            value = {m.key: float(v) for m, v in zip(device.metrics, floats)}

            record = {
                "deviceId": device.device_id,
                "source": device.source,
                "equipmentType": device.equipment_type,
                "building": device.building,
                "ts_utc": int(started * 1000),
                "value": value,
                "metadata": {
                    "msgId": str(uuid.uuid4()),
                    "deviceId": device.device_id,
                    "source": device.source,
                    "topic": device.topic,
                    "unitId": str(device.unit_id),
                    "schema_ver": "v1",
                    **({"energyTag": device.energy_tag} if device.energy_tag else {}),
                    **({"bmsTag": device.bms_tag} if device.bms_tag else {}),
                },
            }
            producer.produce(
                topic=device.topic,
                key=device.device_id.encode("utf-8"),
                value=serializer(record, SerializationContext(device.topic,
                                                              MessageField.VALUE)),
            )
            producer.poll(0)
        except Exception as exc:  # keep the per-device stream resilient
            log.warning("poll failed for %s: %s", device.device_id, exc)

        await asyncio.sleep(max(0.0, interval - (time.time() - started)))


async def run(source: str) -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    host = os.environ.get("MODBUS_HOST", "127.0.0.1")
    port = int(os.environ.get("MODBUS_PORT", "5020"))
    interval = float(os.environ.get("PUBLISH_INTERVAL", "60"))  # 1-min cadence

    devices = devices_for(source)
    producer = _producer()
    serializer = telemetry_serializer()

    client = AsyncModbusTcpClient(host=host, port=port)
    await client.connect()
    log.info("%s publisher connected to %s:%s — starting %d independent device streams",
             source, host, port, len(devices))

    # One independent task (= one IoT stream) per machine.
    tasks = [
        asyncio.create_task(_poll_device(client, d, producer, serializer, interval))
        for d in devices
    ]
    try:
        await asyncio.gather(*tasks)
    finally:
        producer.flush(10)
        client.close()

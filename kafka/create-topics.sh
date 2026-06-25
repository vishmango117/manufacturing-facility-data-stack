#!/usr/bin/env bash
# Creates the per-domain telemetry topics (keyed/partitioned by deviceId so each
# machine is one ordered logical stream). Run inside the cp-kafka image.
set -euo pipefail

BROKER="${KAFKA_BOOTSTRAP:-kafka:9092}"
PARTITIONS="${TOPIC_PARTITIONS:-3}"
RF="${TOPIC_RF:-1}"

TOPICS=(
  bms.hvac.ahu
  bms.hvac.chiller
  bms.hvac.chiller_header
  bms.hvac.cooling_tower
  bms.hvac.air_compressor
  bms.hvac.air_cooler
  ems.machine.injection_moulding
  ems.machine.cnc
  ems.machine.heating
)

echo "Waiting for broker ${BROKER} ..."
until kafka-topics --bootstrap-server "${BROKER}" --list >/dev/null 2>&1; do sleep 3; done

for t in "${TOPICS[@]}"; do
  kafka-topics --bootstrap-server "${BROKER}" --create --if-not-exists \
    --topic "${t}" --partitions "${PARTITIONS}" --replication-factor "${RF}" \
    --config retention.ms=604800000
  echo "  ok: ${t}"
done

echo "Topics:"
kafka-topics --bootstrap-server "${BROKER}" --list

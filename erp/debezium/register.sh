#!/bin/sh
# Registers the Debezium Postgres source + JDBC sink connectors on Kafka Connect.
# POSIX sh + curl only. The JSON files are already in {name, config} POST form.
# Usage: CONNECT_URL=http://connect:8083 sh register.sh
set -eu

CONNECT_URL="${CONNECT_URL:-http://connect:8083}"
DIR="$(cd "$(dirname "$0")" && pwd)"

echo "Waiting for Kafka Connect at ${CONNECT_URL} ..."
until curl -sf "${CONNECT_URL}/connectors" >/dev/null 2>&1; do sleep 3; done

for cfg in postgres-source.json jdbc-sink.json; do
  echo "Registering ${cfg} ..."
  # Idempotent: delete any existing instance, then (re)create.
  name=$(sed -n 's/.*"name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "${DIR}/${cfg}" | head -1)
  curl -s -X DELETE "${CONNECT_URL}/connectors/${name}" >/dev/null 2>&1 || true
  curl -sf -X POST -H "Content-Type: application/json" \
    --data @"${DIR}/${cfg}" "${CONNECT_URL}/connectors" >/dev/null
  echo "  registered: ${name}"
done

echo "Active connectors:"
curl -sf "${CONNECT_URL}/connectors"

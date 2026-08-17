#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092,localhost:9094,localhost:9096}"

echo "Waiting for Kafka at ${BOOTSTRAP}..."
for i in $(seq 1 40); do
  if kafka-topics --bootstrap-server "${BOOTSTRAP}" --list >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

echo "Topic list is filled by later prompts. Kafka is reachable at ${BOOTSTRAP}."
kafka-topics --bootstrap-server "${BOOTSTRAP}" --list

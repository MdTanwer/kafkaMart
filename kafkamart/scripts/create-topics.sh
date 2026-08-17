#!/usr/bin/env bash
# Create KafkaMart topics. Idempotent (--if-not-exists).
# Intended to run inside kafka-1 (or the kafka-init sidecar) which has kafka-topics.
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:19092}"

echo "Waiting for Kafka at ${BOOTSTRAP}..."
ready=0
for _ in $(seq 1 60); do
  if kafka-topics --bootstrap-server "${BOOTSTRAP}" --list >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 2
done
if [[ "${ready}" -ne 1 ]]; then
  echo "Kafka did not become ready at ${BOOTSTRAP}" >&2
  exit 1
fi

create_topic() {
  local topic="$1"
  local partitions="$2"
  local rf="$3"
  shift 3
  local args=(
    --bootstrap-server "${BOOTSTRAP}"
    --create
    --if-not-exists
    --topic "${topic}"
    --partitions "${partitions}"
    --replication-factor "${rf}"
  )
  local cfg
  for cfg in "$@"; do
    args+=(--config "${cfg}")
  done
  kafka-topics "${args[@]}"
  echo "ok  ${topic}  partitions=${partitions} rf=${rf}"
}

create_topic orders            6 3 min.insync.replicas=2 retention.ms=604800000
create_topic users             3 3 cleanup.policy=compact min.insync.replicas=2 min.cleanable.dirty.ratio=0.2
create_topic payments          6 3 min.insync.replicas=2
create_topic inventory-events  6 3 min.insync.replicas=2
create_topic fraud-alerts      3 3 min.insync.replicas=2
create_topic orders-enriched   6 3 min.insync.replicas=2
create_topic shipments         6 3 min.insync.replicas=2
create_topic orders-dlq        3 3 retention.ms=2592000000
create_topic orders-retry-5s   3 3
create_topic orders-retry-1m   3 3
create_topic audit-log         3 3 cleanup.policy=compact
create_topic users-cdc         3 3

echo
echo "Topics:"
kafka-topics --bootstrap-server "${BOOTSTRAP}" --list

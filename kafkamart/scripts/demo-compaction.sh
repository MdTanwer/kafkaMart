#!/usr/bin/env bash
# Compacted-topic demo: 20 updates for one key, roll segments, wait for cleaner.
# Does NOT change create-topics.sh — uses kafka-configs --alter.
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
# kafka-1 internal listener when run via docker exec
INTERNAL_BOOTSTRAP="${KAFKA_INTERNAL_BOOTSTRAP:-localhost:19092}"
API="${USER_PROFILE_URL:-http://localhost:8085}"
USER_ID="${COMPACTION_USER_ID:-ada}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafkamart-kafka-1}"

echo "POST 20 UserProfile updates for user_id=${USER_ID}"
for i in $(seq 1 20); do
  curl -sS -X POST "${API}/api/users" \
    -H "Content-Type: application/json" \
    -d "{\"userId\":\"${USER_ID}\",\"name\":\"Ada Lovelace ${i}\",\"email\":\"ada@kafkamart.dev\"}" \
    >/dev/null
  echo -n "."
done
echo

echo "GET cache (should already be latest):"
curl -sS "${API}/api/users/${USER_ID}"; echo

echo
echo "kafka-configs --alter users segment.ms=60000 min.cleanable.dirty.ratio=0.01 min.compaction.lag.ms=0"
docker exec "${KAFKA_CONTAINER}" kafka-configs --bootstrap-server "${INTERNAL_BOOTSTRAP}" \
  --entity-type topics --entity-name users --alter \
  --add-config segment.ms=60000,min.cleanable.dirty.ratio=0.01,min.compaction.lag.ms=0

echo "Wait 70s for segment roll + cleaner (segment.ms=60s)..."
sleep 70

echo
echo "Avro console consumer --from-beginning (expect ONE record for key=${USER_ID} after compact):"
docker exec kafkamart-schema-registry kafka-avro-console-consumer \
  --bootstrap-server kafka-1:19092 \
  --topic users \
  --from-beginning \
  --timeout-ms 15000 \
  --property schema.registry.url=http://localhost:8181 \
  --property print.key=true \
  --property key.separator=" | " \
  --max-messages 50 || true

echo
echo "Restore a milder dirty ratio (keep compact policy from create-topics.sh):"
docker exec "${KAFKA_CONTAINER}" kafka-configs --bootstrap-server "${INTERNAL_BOOTSTRAP}" \
  --entity-type topics --entity-name users --alter \
  --delete-config segment.ms,min.compaction.lag.ms || true

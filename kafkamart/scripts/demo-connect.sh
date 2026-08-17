#!/usr/bin/env bash
# Converter + SMT + CDC smoke demo (needs Connect plugins + enrichment pipeline).
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
API="${ANALYTICS_URL:-http://localhost:8088}"
PSQL=(docker exec -i kafkamart-postgres psql -U kafkamart -d kafkamart)

echo "=== Connect worker converters (trap) ==="
curl -sS "${CONNECT_URL}/" | python3 -c '
import json,sys
c=json.load(sys.stdin)
print("key.converter  ", c.get("key.converter") or c)
'
echo "Worker value.converter is Avro (compose). JSON topics MUST override per connector."
echo

echo "=== Connector status ==="
curl -sS "${API}/api/analytics/connectors"; echo
echo

echo "=== CDC: upsert a users row in Postgres ==="
curl -sS -X POST "${API}/api/analytics/cdc/users" \
  -H 'Content-Type: application/json' \
  -d '{"userId":"ada","name":"Ada Lovelace","email":"ada@kafkamart.dev"}'
echo
sleep 2
echo "GET /api/analytics/cdc (users-cdc):"
curl -sS "${API}/api/analytics/cdc"; echo
echo
echo "Update name (Debezium UPDATE → users-cdc):"
curl -sS -X POST "${API}/api/analytics/cdc/users" \
  -H 'Content-Type: application/json' \
  -d '{"userId":"ada","name":"Ada Lovelace II","email":"ada@kafkamart.dev"}'
echo
sleep 2
curl -sS "${API}/api/analytics/cdc"; echo
echo
echo "psql users table:"
"${PSQL[@]}" -c 'TABLE users;' || true
echo
echo "JDBC sink table (after an enriched order exists):"
"${PSQL[@]}" -c 'SELECT "orderId","userName","userEmail","ingested_at" FROM enriched_orders LIMIT 5;' || true
echo
echo "ES (schemaless JSON, schema.ignore=true):"
curl -sS "${API}/api/analytics/search?q=ada" || true
echo

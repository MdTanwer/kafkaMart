#!/usr/bin/env bash
# Late-event demo for fraud-detection-service.
#
# Tumbling window = 5 minutes, grace = 1 minute (retention 6 minutes).
# Event time comes from OrderCreated.occurredAt (not the broker timestamp).
#
#   2 minutes late  → still inside grace of the previous/current window → COUNTED
#  20 minutes late  → beyond window + grace (+ store retention)         → DROPPED
#
# "Late" means stream time has already moved on: produce a "now" order first.
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
INTERNAL_BOOTSTRAP="${KAFKA_INTERNAL_BOOTSTRAP:-localhost:19092}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafkamart-kafka-1}"
API="${FRAUD_API:-http://localhost:8086}"
ORDERS_TOPIC="${KAFKA_TOPIC_ORDERS:-orders}"

now_iso() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
iso_ago() { date -u -d "$1" +"%Y-%m-%dT%H:%M:%SZ"; }

if date -u -d "2 minutes ago" >/dev/null 2>&1; then
  NOW="$(now_iso)"
  TWO_MIN="$(iso_ago "2 minutes ago")"
  TWENTY_MIN="$(iso_ago "20 minutes ago")"
else
  # macOS date
  NOW="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  TWO_MIN="$(date -u -v-2M +"%Y-%m-%dT%H:%M:%SZ")"
  TWENTY_MIN="$(date -u -v-20M +"%Y-%m-%dT%H:%M:%SZ")"
fi

COUNTED_USER="late-counted-${RANDOM}"
DROPPED_USER="late-dropped-${RANDOM}"

produce() {
  local key="$1"
  local order_id="$2"
  local user_id="$3"
  local occurred_at="$4"
  local payload
  payload="$(python3 - "$order_id" "$user_id" "$occurred_at" <<'PY'
import json, sys, uuid
order_id, user_id, occurred_at = sys.argv[1], sys.argv[2], sys.argv[3]
print(json.dumps({
  "eventId": str(uuid.uuid4()),
  "occurredAt": occurred_at,
  "traceId": "late-demo",
  "orderId": order_id,
  "userId": user_id,
  "items": [{"sku": "SKU-1", "quantity": 1, "price": 10.00}],
  "totalAmount": 10.00,
  "idempotencyKey": order_id,
  "currency": "USD",
}))
PY
)"
  echo "produce key=${key} occurredAt=${occurred_at} orderId=${order_id}"
  docker exec -i "${KAFKA_CONTAINER}" kafka-console-producer \
    --bootstrap-server "${INTERNAL_BOOTSTRAP}" \
    --topic "${ORDERS_TOPIC}" \
    --property parse.key=true \
    --property key.separator='|' <<EOF
${key}|${payload}
EOF
}

echo "=== LATE EVENT DEMO (stream time advanced by a 'now' order first) ==="
echo "now=${NOW}  2min_late=${TWO_MIN}  20min_late=${TWENTY_MIN}"
echo

echo "--- COUNTED path: two on-time + one 2-min-late for ${COUNTED_USER} ---"
produce "${COUNTED_USER}" "cnt-now-1" "${COUNTED_USER}" "${NOW}"
produce "${COUNTED_USER}" "cnt-now-2" "${COUNTED_USER}" "${NOW}"
produce "${COUNTED_USER}" "cnt-2m" "${COUNTED_USER}" "${TWO_MIN}"
echo "EXPECTED: 2 min late is within 1-minute grace → counted → VELOCITY alert"
echo

echo "--- DROPPED path: one on-time (advance stream time) + three 20-min-late for ${DROPPED_USER} ---"
produce "${DROPPED_USER}" "drp-now" "${DROPPED_USER}" "${NOW}"
produce "${DROPPED_USER}" "drp-20m-1" "${DROPPED_USER}" "${TWENTY_MIN}"
produce "${DROPPED_USER}" "drp-20m-2" "${DROPPED_USER}" "${TWENTY_MIN}"
produce "${DROPPED_USER}" "drp-20m-3" "${DROPPED_USER}" "${TWENTY_MIN}"
echo "EXPECTED: 20 min late is beyond 5m window + 1m grace (retention 6m) → DROPPED, no VELOCITY"
echo

echo "Waiting 3s for Streams..."
sleep 3

echo
echo "GET ${API}/api/fraud/alerts (look for ${COUNTED_USER}; ${DROPPED_USER} should be absent):"
curl -sS "${API}/api/fraud/alerts" | python3 -c '
import json, sys
user_c, user_d = sys.argv[1], sys.argv[2]
alerts = json.load(sys.stdin)
def show(u):
    hits = [a for a in alerts if a.get("userId")==u]
    print(f"  user={u} alerts={len(hits)} reasons={[a.get("reason") for a in hits]}")
show(user_c)
show(user_d)
' "${COUNTED_USER}" "${DROPPED_USER}"

echo
echo "Service logs (LATE_CANDIDATE + Skipping record for expired window):"
echo "  journal/quarkus: grep LATE_CANDIDATE and 'Skipping record for expired window'"
echo "  docker: docker logs kafkamart-fraud 2>&1 | grep -E 'LATE_CANDIDATE|expired window|FRAUD_ALERT'"
echo
echo "bootstrap=${BOOTSTRAP}  (host produce unused; used docker exec ${KAFKA_CONTAINER})"

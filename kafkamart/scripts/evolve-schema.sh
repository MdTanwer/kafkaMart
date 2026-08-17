#!/usr/bin/env bash
# Register UserProfile v2 (BACKWARD-compatible phone) and prove a breaking schema is 409'd.
# Usage: ./scripts/evolve-schema.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SR="${SCHEMA_REGISTRY_URL:-http://localhost:8181}"
SUBJECT="${SCHEMA_SUBJECT:-users-value}"
V1="${ROOT}/infra/schemas/UserProfile_v1.avsc"
V2="${ROOT}/infra/schemas/UserProfile_v2.avsc"

body_from_avsc() {
  python3 - "$1" <<'PY'
import json, pathlib, sys
schema = pathlib.Path(sys.argv[1]).read_text()
print(json.dumps({"schema": schema, "schemaType": "AVRO"}))
PY
}

breaking_body() {
  python3 - <<'PY'
import json
schema = {
  "type": "record",
  "name": "UserProfile",
  "namespace": "com.kafkamart.avro",
  "fields": [
    {"name": "user_id", "type": "string"},
    {"name": "name", "type": "string"}
  ]
}
print(json.dumps({"schema": json.dumps(schema), "schemaType": "AVRO"}))
PY
}

echo "Schema Registry: ${SR}  subject: ${SUBJECT}"
echo "Waiting for Schema Registry..."
for _ in $(seq 1 30); do
  if curl -sf "${SR}/subjects" >/dev/null; then
    break
  fi
  sleep 1
done
curl -sf "${SR}/subjects" >/dev/null

echo
echo "== 1. Ensure v1 is registered (idempotent) =="
code=$(curl -sS -o /tmp/sr-v1.json -w "%{http_code}" \
  -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "$(body_from_avsc "${V1}")" \
  "${SR}/subjects/${SUBJECT}/versions")
echo "POST v1 → HTTP ${code}"
cat /tmp/sr-v1.json; echo

echo
echo "== 2. BACKWARD compatibility check for v2 (optional phone) =="
code=$(curl -sS -o /tmp/sr-compat-v2.json -w "%{http_code}" \
  -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "$(body_from_avsc "${V2}")" \
  "${SR}/compatibility/subjects/${SUBJECT}/versions/latest")
echo "POST compatibility v2 → HTTP ${code}"
cat /tmp/sr-compat-v2.json; echo
python3 - <<'PY'
import json
data = json.load(open("/tmp/sr-compat-v2.json"))
ok = data.get("is_compatible") is True
print("BACKWARD check:", "PASS" if ok else "FAIL")
raise SystemExit(0 if ok else 1)
PY

echo
echo "== 3. Register v2 =="
code=$(curl -sS -o /tmp/sr-v2.json -w "%{http_code}" \
  -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "$(body_from_avsc "${V2}")" \
  "${SR}/subjects/${SUBJECT}/versions")
echo "POST v2 → HTTP ${code}"
cat /tmp/sr-v2.json; echo

echo
echo "== 4. Breaking change: delete email (FULL check so running v1 consumers are protected) =="
curl -sS -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"compatibility":"FULL"}' \
  "${SR}/config/${SUBJECT}" >/tmp/sr-full.json
echo "subject compatibility → FULL ($(cat /tmp/sr-full.json))"

code=$(curl -sS -o /tmp/sr-break.json -w "%{http_code}" \
  -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "$(breaking_body)" \
  "${SR}/subjects/${SUBJECT}/versions")
echo "POST delete-email → HTTP ${code}  (expect 409)"
cat /tmp/sr-break.json; echo

curl -sS -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"compatibility":"BACKWARD"}' \
  "${SR}/config/${SUBJECT}" >/dev/null
echo "subject compatibility restored to BACKWARD"

if [[ "${code}" != "409" ]]; then
  echo "ERROR: expected HTTP 409 for breaking schema, got ${code}" >&2
  exit 1
fi
echo
echo "OK: v2 is BACKWARD-compatible; deleting email is rejected with 409."

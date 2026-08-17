#!/usr/bin/env bash
# POST connector JSON from infra/connectors to the Connect REST API.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
DIR="${KAFKAMART_CONNECTORS_DIR:-${ROOT}/infra/connectors}"

echo "Waiting for Connect at ${CONNECT_URL}..."
ready=0
for _ in $(seq 1 60); do
  if curl -sf "${CONNECT_URL}/" >/dev/null; then
    ready=1
    break
  fi
  sleep 2
done
if [[ "${ready}" -ne 1 ]]; then
  echo "Connect is not up at ${CONNECT_URL}" >&2
  exit 1
fi

apply() {
  local file="$1"
  local name
  name="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["name"])' "${file}")"
  echo "PUT ${name} from $(basename "${file}")"
  python3 - "${CONNECT_URL}" "${file}" "${name}" <<'PY'
import json, sys, urllib.request
base, path, name = sys.argv[1], sys.argv[2], sys.argv[3]
cfg = json.load(open(path))
body = json.dumps(cfg["config"]).encode()
req = urllib.request.Request(
    f"{base}/connectors/{name}/config",
    data=body,
    method="PUT",
    headers={"Content-Type": "application/json"},
)
try:
    with urllib.request.urlopen(req) as resp:
        print(resp.status, resp.read()[:200].decode())
except urllib.error.HTTPError as err:
    print(err.status, err.read().decode()[:500], file=sys.stderr)
    raise
PY
}

for f in \
  "${DIR}/jdbc-sink-orders-enriched.json" \
  "${DIR}/elasticsearch-sink-orders-enriched.json" \
  "${DIR}/debezium-postgres-users.json"
do
  apply "${f}"
done

echo
echo "Connectors:"
curl -sS "${CONNECT_URL}/connectors?expand=status" | python3 -m json.tool | head -80

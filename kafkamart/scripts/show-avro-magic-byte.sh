#!/usr/bin/env bash
# Print Confluent Avro wire format: 0x00 + 4-byte big-endian schema id.
set -euo pipefail

echo "Consuming one raw users record (kcat if available, else python + kafka isn't required)."
echo "Prefer: docker run edenhill/kcat against the compose network."
echo

if docker image inspect edenhill/kcat:1.7.1 >/dev/null 2>&1 || docker pull edenhill/kcat:1.7.1; then
  docker run --rm --network kafkamart-net edenhill/kcat:1.7.1 \
    -b kafka-1:19092 -C -t users -c 1 -s value=raw -u \
    | python3 -c '
import sys
b = sys.stdin.buffer.read()
if len(b) < 5:
    print("short payload", len(b)); sys.exit(1)
magic, schema_id = b[0], int.from_bytes(b[1:5], "big")
print(f"bytes={len(b)} magic=0x{magic:02x} schema_id={schema_id}")
print("hex:", b[:16].hex(" "))
assert magic == 0, "magic byte must be 0x00"
print("OK: Confluent wire format")
'
else
  echo "Install kcat or run UserProfileTest.avroWireFormatHasMagicByteAndSchemaId"
fi

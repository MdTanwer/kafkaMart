# fraud-detection-service

HTTP port: `8086` | Prompt: **S6 — Kafka Streams state**

## Kafka concept this service teaches

**Local state is a cache of a changelog topic.** Kafka Streams keeps a RocksDB window store on disk (`state.dir`) and mirrors every update to an internal compacted **changelog**. Crash the JVM, delete the local files, restart: Streams **restores counts from the changelog** before it processes new records.

This topology has two branches on `orders`:

| Branch | Kind | Rule | Output |
| --- | --- | --- | --- |
| High-value | **Stateless** `filter` | `totalAmount > 10000` (exactly 10000 is not fraud) | `FraudAlert` `HIGH_VALUE`, key = `userId` |
| Velocity | **Stateful** `groupByKey` → tumbling **5 min**, grace **1 min** → `count()` | `count >= 3` | `FraudAlert` `VELOCITY` |

Event time is `OrderCreated.occurredAt` (`OrderCreatedTimestampExtractor`), not the broker timestamp — required for the late-event demo.

### Why we do not `suppress()`

Fraud should fire on the **3rd order**, not when the 5-minute window closes. `filter(count >= 3)` already drops counts 1 and 2. `suppress(untilWindowCloses)` would delay the alert by up to window + grace. Counts 4, 5, … emit more `VELOCITY` alerts on purpose (the burst is still happening).

`selectKey` to `userId` is intentional even though order-api already keys by `userId`: Streams treats the key as changed and **auto-creates a repartition topic**.

Internal names (application-id `fraud-detection-service`):

```
fraud-detection-service-orders-by-user-repartition
fraud-detection-service-order-velocity-5m-changelog
```

`auto.create.topics.enable=false` does **not** block these: Streams creates them via AdminClient.

## Run

```bash
docker compose up -d
./scripts/create-topics.sh   # or wait for kafka-init
export JAVA_HOME=/data/tools/jdk-25
./mvnw -pl services/fraud-detection-service -am quarkus:dev
```

Or JVM image:

```bash
docker compose --profile apps up -d --build fraud
```

State directory: `FRAUD_STATE_DIR` (default `/tmp/fraud-state`).

## REST

- `GET /api/fraud/alerts` — last 100 `FraudAlert` records from topic `fraud-alerts` (consumer group `fraud-alerts-reader`)

```bash
curl -sS http://localhost:8086/api/fraud/alerts
```

## Live velocity alert

Three orders for the same user inside five minutes (amount ≤ 10000 so you only see `VELOCITY`):

```bash
USER=vel-demo
for i in 1 2 3; do
  curl -sS -X POST http://localhost:8080/api/orders \
    -H 'Content-Type: application/json' \
    -H "Idempotency-Key: vel-$USER-$i" \
    -d "{\"userId\":\"$USER\",\"items\":[{\"sku\":\"SKU-1\",\"quantity\":1,\"price\":9.99}],\"currency\":\"USD\"}"
  echo
done
sleep 2
curl -sS http://localhost:8086/api/fraud/alerts
# expect reason=VELOCITY, userId=vel-demo
```

High-value: POST an order whose items sum to `10000.01`.

## RocksDB files on disk

After the topology has processed at least one order:

```bash
STATE_DIR="${FRAUD_STATE_DIR:-/tmp/fraud-state}"
find "$STATE_DIR" -type f | head
# typical layout:
#   /tmp/fraud-state/fraud-detection-service/<task>/rocksdb/order-velocity-5m/{CURRENT,IDENTITY,MANIFEST-*,*.sst,LOG}
ls -la "$STATE_DIR"/fraud-detection-service/*/rocksdb/order-velocity-5m/
```

In Compose: `docker exec kafkamart-fraud ls -la /tmp/fraud-state/fraud-detection-service/*/rocksdb/order-velocity-5m/`

## Auto-created changelog + repartition topics

```bash
docker exec kafkamart-kafka-1 kafka-topics --bootstrap-server localhost:19092 --list | grep fraud-detection-service
```

Proof (names Streams creates for this topology):

```
fraud-detection-service-order-velocity-5m-changelog
fraud-detection-service-orders-by-user-repartition
```

Describe the changelog (`cleanup.policy=compact`, RF=3 in compose):

```bash
docker exec kafkamart-kafka-1 kafka-topics --bootstrap-server localhost:19092 \
  --describe --topic fraud-detection-service-order-velocity-5m-changelog
```

## Crash-recovery demo (changelog restore)

Counts live in RocksDB **and** in the changelog. `kill -9` the JVM, **delete `state.dir`**, restart: the next order continues the window (restore from changelog), it does not start at 1.

```bash
export FRAUD_STATE_DIR=/tmp/fraud-state
USER=crash-demo

# 1. Run the service, then produce TWO orders (count=2, no VELOCITY yet).
for i in 1 2; do
  curl -sS -X POST http://localhost:8080/api/orders \
    -H 'Content-Type: application/json' \
    -H "Idempotency-Key: crash-$USER-$i" \
    -d "{\"userId\":\"$USER\",\"items\":[{\"sku\":\"SKU-1\",\"quantity\":1,\"price\":9.99}],\"currency\":\"USD\"}"
  echo
done
sleep 2
curl -sS http://localhost:8086/api/fraud/alerts | grep -c crash-demo || true   # 0 velocity alerts

ls "$FRAUD_STATE_DIR"/fraud-detection-service/*/rocksdb/order-velocity-5m/

# 2. Hard-kill the JVM (not SIGTERM). Then wipe local RocksDB so restore MUST use the changelog.
kill -9 "$(pgrep -f fraud-detection-service | head -1)"
rm -rf "$FRAUD_STATE_DIR"

# 3. Restart quarkus:dev (or the fraud container). Watch restore:
#    "Restoring state from changelog fraud-detection-service-order-velocity-5m-changelog"

# 4. Third order in the same 5-minute window → VELOCITY (count survived as 2+1).
curl -sS -X POST http://localhost:8080/api/orders \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: crash-$USER-3" \
  -d "{\"userId\":\"$USER\",\"items\":[{\"sku\":\"SKU-1\",\"quantity\":1,\"price\":9.99}],\"currency\":\"USD\"}"
sleep 2
curl -sS http://localhost:8086/api/fraud/alerts
# expect reason=VELOCITY for crash-demo — proof the window count was restored
```

If you `kill -9` **without** deleting `state.dir`, Streams reopens the local RocksDB (faster). Wiping the dir is the changelog-restore proof.

## Late-event demo

```bash
chmod +x scripts/demo-fraud-late.sh
./scripts/demo-fraud-late.sh
```

The script produces JSON directly to `orders` (order-api always stamps `occurredAt=now`).

| Record | `occurredAt` | Outcome |
| --- | --- | --- |
| 2 min late | within 5m window + 1m grace | **COUNTED** — third event can fire `VELOCITY`; log `LATE_CANDIDATE` |
| 20 min late | beyond window + grace (store retention is 6 min) | **DROPPED** — log `Skipping record for expired window`; no `VELOCITY` |

Always produce a **now** order first so stream time has moved past the old timestamp.

```bash
docker logs kafkamart-fraud 2>&1 | grep -E 'LATE_CANDIDATE|expired window|FRAUD_ALERT'
```

## Config

| Key | Default / env |
| --- | --- |
| `quarkus.kafka-streams.bootstrap-servers` | `KAFKA_BOOTSTRAP_SERVERS` |
| `quarkus.kafka-streams.application-id` | `fraud-detection-service` |
| `kafka-streams.replication.factor` | `3` (`1` in `%test`) |
| `kafka-streams.processing.guarantee` | `exactly_once_v2` (`at_least_once` in `%test` — single-broker Dev Services) |
| `quarkus.kafka-streams.topics` | `orders,fraud-alerts` |
| `kafka-streams.state.dir` | `FRAUD_STATE_DIR` → `/tmp/fraud-state` |
| `kafka-streams.commit.interval.ms` | `1000` |

Downstream `fraud-alerts` reader uses `isolation.level=read_committed` because prod Streams uses EOS v2.

## Tests

```bash
export JAVA_HOME=/data/tools/jdk-25
./mvnw -pl services/fraud-detection-service -am verify
```

`FraudTopologyTest` uses **TopologyTestDriver** (no broker): 3rd in-window order → `VELOCITY`; two + two across the 5-minute boundary → none.

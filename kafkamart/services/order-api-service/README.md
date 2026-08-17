# order-api-service

HTTP port: `8080` | Prompt: **S1 — producer internals**

## Kafka concept this service teaches

How a **producer** actually writes to Kafka:

- **Record key = `userId`** so all of one customer's orders share a partition (per-user ordering).
- **`acks=all` + `enable.idempotence=true` + `retries=Integer.MAX_VALUE` + `max.in.flight.requests.per.connection=5`** — the safe idempotent-producer set (no duplicates, no reordering).
- **Batching**: `linger.ms=20`, `batch.size=65536`, `compression.type=lz4`.
- **Custom `VipPartitioner`**: keys `vip-*` are pinned to **partition 0**; everyone else is `hash(key) % remaining partitions` (1..n-1). Same user → same partition.

## Run

Infra (from repo root):

```bash
docker compose up -d
./scripts/create-topics.sh   # or wait for kafka-init
./mvnw -pl services/order-api-service -am quarkus:dev
```

Or JVM image:

```bash
docker compose --profile apps up -d --build order-api
```

## API

`POST /api/orders` — body `{ userId, items[{sku,quantity,price}], totalAmount?, currency? }`

- Validates with Hibernate Validator
- Builds `OrderCreated` (eventId / occurredAt / traceId from `kafkamart-common`)
- Emits to channel `orders-out` with Kafka key = `userId`
- Returns **202** `{ "orderId": "..." }`
- Header `Idempotency-Key`: same key returns the stored `orderId` and does **not** produce again

`GET /api/orders/simulate-load?count=N` — fires N orders round-robin across `user-alice`, `user-bob`, `vip-carol`.

`traceId` request header is copied onto the Kafka record header of the same name.

## Demo — prove same user → same partition

Produce **10** orders for one user (distinct idempotency keys so they are not deduped):

```bash
for i in $(seq 1 10); do
  curl -sS -X POST http://localhost:8080/api/orders \
    -H 'Content-Type: application/json' \
    -H "Idempotency-Key: demo-user-ada-$i" \
    -H "traceId: demo-trace-$i" \
    -d '{"userId":"user-ada","items":[{"sku":"SKU-1","quantity":1,"price":9.99}]}'
  echo
done
```

**GetOffsetShell** — high-watermark per partition. After 10 messages for one user, **one** partition's offset jumps by 10; the others stay put:

```bash
docker exec kafkamart-kafka-1 kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:19092 \
  --topic orders
```

**console-consumer `--print.offsets`** — every line should show the **same partition** and the key `user-ada`:

```bash
docker exec kafkamart-kafka-1 kafka-console-consumer \
  --bootstrap-server localhost:19092 \
  --topic orders \
  --from-beginning \
  --max-messages 10 \
  --timeout-ms 15000 \
  --property print.key=true \
  --property print.partition=true \
  --property print.offset=true \
  --property print.offsets=true \
  --property key.separator=' | '
```

Acceptance checks on the docker cluster (`orders` has 6 partitions):

```bash
# 6 orders, same user → one partition
for i in $(seq 1 6); do
  curl -sS -X POST http://localhost:8080/api/orders \
    -H 'Content-Type: application/json' \
    -H "Idempotency-Key: same-user-$i" \
    -d '{"userId":"user-ada","items":[{"sku":"SKU-1","quantity":1,"price":9.99}]}'
  echo
done

# VIP user → partition 0
curl -sS -X POST http://localhost:8080/api/orders \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: vip-1' \
  -d '{"userId":"vip-ada","items":[{"sku":"SKU-1","quantity":1,"price":9.99}]}'
echo
```

Re-run the console-consumer (or Kafka-UI at http://localhost:9000) and confirm `user-ada` rows share a partition **≠ 0**, and `vip-ada` is on **partition 0**. Enable partitioner logs in `%dev`: look for `user-ada → <n>` / `vip-ada → 0`.

## Other probes

```bash
curl -s localhost:8080/q/health/live
curl -s localhost:8080/q/health/ready    # fails if Kafka is down
curl -s localhost:8080/q/metrics | grep orders_produced
curl -s localhost:8080/q/openapi
```

## Config

Broker/topic values come from environment variables. Defaults:

`KAFKA_BOOTSTRAP_SERVERS=localhost:9092,localhost:9094,localhost:9096`

`KAFKA_TOPIC_ORDERS=orders`

# inventory-service

HTTP port: `8081` | Prompt: **S2 — consumer internals + idempotent consumption**

## Kafka concept this service teaches

How a **consumer** stays correct under redelivery and rebalance:

- **`enable.auto.commit=false`** — Kafka does not mark a record done just because it was polled.
- **Manual `Message.ack()`** only after the DB transaction commits. `nack(failure)` on error.
- **Idempotent processing**: `ProcessedOffset(partition, offset, orderId)` with `UNIQUE(partition, offset)` lives in the **same `@Transactional`** as the stock decrement. Crash after DB write, before ack → the broker redelivers → the unique row makes it a **skip + ack**. No double decrement.
- **`KafkaConsumerRebalanceListener` (`@Identifier("orders-in")`)**: `onPartitionsAssigned` logs the new assignment; `onPartitionsRevoked` logs and **`commitSync()`** flushes commits before partitions move.

## Channel knobs (`orders-in`)

| Setting | What it does |
| --- | --- |
| `auto.offset.reset=earliest` | New group `inventory-service` reads the topic from the beginning (catch-up / demo). |
| `fetch.min.bytes=1024` | Broker waits until at least 1 KiB is available (fewer tiny fetches). |
| `fetch.max.wait.ms=500` | Upper bound on that wait so latency stays bounded. |
| `max.partition.fetch.bytes=1048576` | Cap on bytes returned per partition per fetch (1 MiB). |
| `max.poll.records=500` | Max records per `poll()` — smaller batches → faster ack cycles. |
| `max.poll.interval.ms=300000` | If processing a batch takes longer than 5 min, the member is kicked out (rebalance). |
| `enable.auto.commit=false` | Offset moves only when we `ack()` after the DB write. |
| `commit-strategy=throttled` | SmallRye commits consecutive acked offsets (no holes). |

Outgoing `inventory-out` uses `acks=all` + `enable.idempotence=true`, key = `orderId`.

## Run

```bash
docker compose up -d
./mvnw -pl services/inventory-service -am quarkus:dev
```

Needs Kafka **and** Postgres (`POSTGRES_JDBC_URL`, default `jdbc:postgresql://localhost:5432/kafkamart`).

Or JVM image:

```bash
docker compose --profile apps up -d --build inventory
```

## REST

- `GET /api/inventory/{sku}` — on-hand quantity
- `POST /api/inventory/{sku}/restock?qty=N` — add stock (creates the SKU if needed)

Startup seeds `SKU-1=100` and `SKU-LOAD=1000`.

## Demo — reserve, then prove crash-before-ack does not double-decrement

### 1. Place an order via S1 and watch stock + `inventory-events`

```bash
curl -sS -X POST 'http://localhost:8081/api/inventory/SKU-1/restock?qty=50'
echo
curl -sS http://localhost:8081/api/inventory/SKU-1
echo

curl -sS -X POST http://localhost:8080/api/orders \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: inv-demo-1' \
  -d '{"userId":"user-ada","items":[{"sku":"SKU-1","quantity":2,"price":9.99}]}'
echo

sleep 2
curl -sS http://localhost:8081/api/inventory/SKU-1
echo

docker exec kafkamart-kafka-1 kafka-console-consumer \
  --bootstrap-server localhost:19092 \
  --topic inventory-events \
  --from-beginning \
  --max-messages 1 \
  --timeout-ms 10000 \
  --property print.key=true \
  --property print.value=true
```

You should see stock **decremented by 2** and an `InventoryReserved` with `"status":"RESERVED"`.

Watch rebalance logs on startup / rolling restart:

```
REBALANCE assigned group=inventory-service partitions=[...]
REBALANCE revoked group=inventory-service partitions=[...] — flushing commits
```

### 2. `kill -9` mid-batch → restart → no duplicate decrement

```bash
# note current stock
curl -sS http://localhost:8081/api/inventory/SKU-1
echo

# fire a burst (distinct idempotency keys) then immediately kill the JVM
for i in $(seq 1 20); do
  curl -sS -X POST http://localhost:8080/api/orders \
    -H 'Content-Type: application/json' \
    -H "Idempotency-Key: crash-$i" \
    -d '{"userId":"user-ada","items":[{"sku":"SKU-1","quantity":1,"price":9.99}]}' >/dev/null &
done
wait
# if running via quarkus:dev / java -jar:
kill -9 $(pgrep -f inventory-service) || docker kill -s KILL kafkamart-inventory

# restart
./mvnw -pl services/inventory-service -am quarkus:dev
# or: docker compose --profile apps up -d inventory

sleep 5
curl -sS http://localhost:8081/api/inventory/SKU-1
echo
```

Compare stock to (starting quantity − number of **unique** orders that actually landed). Restarting must **not** subtract again for records whose `ProcessedOffset` row already exists. Duplicate redeliveries log `DEDUP skip+ack partition=… offset=…`.

## Other probes

```bash
curl -s localhost:8081/q/health/live
curl -s localhost:8081/q/health/ready
curl -s localhost:8081/q/metrics | grep inventory
curl -s localhost:8081/q/openapi
```

## Config

`KAFKA_BOOTSTRAP_SERVERS=localhost:9092,localhost:9094,localhost:9096`

`KAFKA_TOPIC_ORDERS=orders` · `KAFKA_TOPIC_INVENTORY_EVENTS=inventory-events`

`POSTGRES_JDBC_URL=jdbc:postgresql://localhost:5432/kafkamart` · `POSTGRES_USER` / `POSTGRES_PASSWORD=kafkamart`

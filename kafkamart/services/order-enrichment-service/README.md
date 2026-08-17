# order-enrichment-service

HTTP port: `8087` | Prompt: **S7 — stream-table joins, GlobalKTable, Interactive Queries**

## Kafka concept this service teaches

A **KStream** is a changelog of facts (`orders`). A **KTable** is the latest value per key (`users`, compacted). A **KStream-KTable join** is a *stream lookup against the current table*: each order is enriched with whatever profile is in the table **right now**.

Interactive Queries (IQ) expose that state over HTTP: `GET /api/enrichment/orders/{orderId}` reads the local RocksDB store `orders-store` instead of consuming `orders-enriched`. `application.server=0.0.0.0:8087` is advertised in Streams metadata so another instance can tell clients **which host** owns a key.

## Run

```bash
docker compose up -d
export JAVA_HOME=/data/tools/jdk-25
./mvnw -pl services/order-enrichment-service -am quarkus:dev
```

Needs Kafka **and** Schema Registry (`SCHEMA_REGISTRY_URL`, default `http://localhost:8181`) — `users` is Avro.

```bash
docker compose --profile apps up -d --build enrichment
```

## Topology

1. `KTable<String, UserProfile> users` from compacted `users` (Avro via Schema Registry).
2. `KStream<String, OrderCreated> orders` from `orders` (JSON).
3. **Inner** join on `userId` → `EnrichedOrder` → `orders-enriched` keyed by **orderId**.
4. Same stream is materialized as queryable store **`orders-store`** (latest `EnrichedOrder` per `orderId`).

Event time is not the lesson here; the table is.

### Inner-join semantics (order before profile)

If the order arrives **before** the profile is in the KTable, the join **drops** the order. When the profile shows up later, Streams does **not** replay that order. Send a **new** order after the table has the user.

This is not a bug. KStream-KTable join is not KTable-KTable (which would retract/update) and not a left join.

## Copartitioning trap (`users`=3 vs `orders`=6)

Kafka Streams **KStream-KTable** joins require **copartitioning**: same join key **and the same number of partitions**. `scripts/create-topics.sh` creates `orders` with **6** partitions and `users` with **3**.

| Pattern | What happens |
| --- | --- |
| **Non-key join** (stream keyed by `orderId`, join on `userId`) | You `selectKey(userId)`. The auto-created repartition topic **inherits 6 partitions from `orders`**. Join against `users`(3) → startup failure: *Topics not copartitioned*. |
| **Key-based join** (both already keyed by `userId`) | Same key space, but **6 vs 3 still fails** the copartition check. Hash(`userId`) % 6 ≠ hash(`userId`) % 3, and each task only sees **one** table partition. |
| **This service** | `selectKey(userId)` then `Repartitioned.withNumberOfPartitions(3)` so the join input matches `users`. The join is legal; the trap is thinking “same key ⇒ partitions don’t matter.” |

Internal topic: `order-enrichment-service-orders-by-user-repartition` (3 partitions).

```bash
docker exec kafkamart-kafka-1 kafka-topics --bootstrap-server localhost:19092 --describe \
  --topic order-enrichment-service-orders-by-user-repartition
```

### When GlobalKTable is the right choice

`GlobalKTable` **replicates the entire table to every instance**. No copartitioning. Join key is extracted with a mapper (`order.userId()`), so the stream can stay keyed by `orderId`.

Use it when:

- you cannot or will not align partition counts
- you join on a **non-key** field and do not want a repartition topic
- the table is **small** (user profiles yes; clickstream no)

Cost: every `users` update is **broadcast** (N instances × full changelog). Memory = full table per JVM.

Toggle (do not enable both in prod without re-checking state stores):

```java
// EnrichmentTopology.USE_GLOBAL_KTABLE = true;
```

or `ENRICHMENT_JOIN_MODE=global`.

## Interactive Queries

```bash
curl -sS http://localhost:8087/api/enrichment/orders/{orderId}
```

- **200** `{..., "userName":"...", "userEmail":"..."}` when this instance hosts the key
- **404** `{"orderId":"...","host":"0.0.0.0","port":8087,"reason":"..."}` when missing, or when another instance owns the key (`host` is the RPC address from `application.server`)

`quarkus.kafka-streams.application-server=${ENRICHMENT_APPLICATION_SERVER:0.0.0.0:8087}`

Quarkus 3.33 has no `io.quarkus.kafka.streams.runtime.InteractiveQueries` bean. This service injects `KafkaStreams` into `com.kafkamart.enrichment.InteractiveQueries` and reads `ReadOnlyKeyValueStore` `orders-store`.

## Living table demo

Update the profile via **S5**, then place a **new** order. Enrichment uses the **new** name/email immediately (KTable = latest per key). The previous order’s `EnrichedOrder` is unchanged.

```bash
# 1. Profile
curl -sS -X POST http://localhost:8085/api/users \
  -H 'Content-Type: application/json' \
  -d '{"userId":"ada","name":"Ada Lovelace","email":"ada@kafkamart.dev"}'
echo

# 2. Order (order-api keys by userId)
curl -sS -X POST http://localhost:8080/api/orders \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: ada-1' \
  -d '{"userId":"ada","items":[{"sku":"SKU-1","quantity":1,"price":9.99}],"currency":"USD"}'
echo
# wait, then IQ — userName=Ada Lovelace
# curl -sS http://localhost:8087/api/enrichment/orders/<orderId>

# 3. Rename
curl -sS -X POST http://localhost:8085/api/users \
  -H 'Content-Type: application/json' \
  -d '{"userId":"ada","name":"Ada Lovelace II","email":"ada@kafkamart.dev"}'
echo

# 4. NEW order (the old one stays Ada Lovelace)
curl -sS -X POST http://localhost:8080/api/orders \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: ada-2' \
  -d '{"userId":"ada","items":[{"sku":"SKU-1","quantity":1,"price":9.99}],"currency":"USD"}'
echo
# IQ for the new orderId → userName=Ada Lovelace II
```

## Config

| Key | Default / env |
| --- | --- |
| `quarkus.kafka-streams.application-id` | `order-enrichment-service` |
| `quarkus.kafka-streams.application-server` | `ENRICHMENT_APPLICATION_SERVER` → `0.0.0.0:8087` |
| `quarkus.kafka-streams.topics` | `orders,users,orders-enriched` |
| `kafka-streams.replication.factor` | `3` (`1` in `%test`) |
| `kafka-streams.processing.guarantee` | `exactly_once_v2` (`at_least_once` in `%test`) |
| `kafka-streams.schema.registry.url` | `SCHEMA_REGISTRY_URL` |
| `enrichment.users.partitions` | `3` (must match `users`) |
| `enrichment.join.mode` | `ktable` or `global` |

## Tests

```bash
export JAVA_HOME=/data/tools/jdk-25
./mvnw -pl services/order-enrichment-service -am verify
```

`EnrichmentTopologyTest` (TopologyTestDriver, mock Schema Registry):

- profile in table, then order → `EnrichedOrder` + `orders-store`
- **order before profile → no output; later profile does not replay the order**
- living table: second order sees the renamed profile
- GlobalKTable variant still joins

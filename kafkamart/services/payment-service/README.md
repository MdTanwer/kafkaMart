# payment-service

HTTP port: `8082` | Prompt: **S3 — transactions + exactly-once (EOS)**

## Kafka concept this service teaches

**Consume-transform-produce inside one Kafka transaction.** An `OrderCreated` on `orders` becomes a `PaymentCompleted` on `payments`. The outgoing record **and** the consumed offset are committed together (or both abort). Downstream consumers **must** set `isolation.level=read_committed` or they will see records from aborted transactions.

### SmallRye 4.33 — attribute names that do **not** exist

Verified against `smallrye-reactive-messaging-kafka` **4.33.0** (`KafkaConnectorIncomingConfiguration` / `KafkaConnectorOutgoingConfiguration`):

| Prompt name | In 4.33? | What we use instead |
| --- | --- | --- |
| `mp.messaging.incoming.orders-in.exactly-once=true` | **No** | `KafkaTransactions.withTransactionAndAck` |
| `mp.messaging.outgoing.payments-out.transactional=true` | **No** | `mp.messaging.outgoing.payments-out.transactional.id=...` |
| `mp.messaging.incoming.orders-in.commit-strategy=ignore` | **Yes** | Required so the connector does not commit offsets outside the TX |
| `transactional.id=payment-tx-${INSTANCE_ID:local-1}` | **Yes** | Unique per running instance |

A plain `@Incoming` + `@Outgoing` processor would produce and ack independently (**at-least-once**). We do **not** fall back to that. The processor is `@Incoming("orders-in")` plus `@Channel("payments-out") KafkaTransactions<PaymentCompleted>`.

Gateway rule (deterministic by amount): **amount &lt; 10000 → `PAID`**, else `FAILED`. Kafka key on `payments` is `orderId`. `transactionId` is `pay-{orderId}` so a retried TX is the same logical payment.

## Run

```bash
docker compose up -d
./mvnw -pl services/payment-service -am quarkus:dev
```

Or JVM image:

```bash
docker compose --profile apps up -d --build payment
```

`INSTANCE_ID` defaults to `local-1` (`transactional.id=payment-tx-local-1`). Compose sets `INSTANCE_ID=payment-1`.

## REST

- `GET /api/payments/config` — effective EOS settings (commit-strategy, transactional.id, required downstream isolation)
- `POST /api/payments/chaos/crash` — arm a **one-shot** crash **after** the payment record is sent and **before** the Kafka TX commits (`%dev` / `%test` only)

## Demo — crash mid-TX, restart, no duplicates

`failure-strategy=ignore` resets the consumer to the last **committed** offset after a nack. That is the same recovery path as kill + restart: the aborted TX is not visible to `read_committed`, the offset was never committed, the order is processed once more, one `PaymentCompleted` lands.

```bash
# 1. Inspect EOS config
curl -sS http://localhost:8082/api/payments/config | jq .

# 2. Arm chaos, then place an order (S1 on 8080)
curl -sS -X POST http://localhost:8082/api/payments/chaos/crash
echo

curl -sS -X POST http://localhost:8080/api/orders \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: pay-eos-1' \
  -d '{"userId":"user-ada","items":[{"sku":"SKU-1","quantity":1,"price":12.00}]}'
echo

# Optional: kill the process here to demo restart. Offset is uncommitted; TX is aborted.
# ./mvnw -pl services/payment-service -am quarkus:dev   # start again

# 3. Count committed payments for that order (MUST use read_committed)
docker exec kafkamart-kafka-1 kafka-console-consumer \
  --bootstrap-server localhost:19092 \
  --topic payments \
  --from-beginning \
  --max-messages 5 \
  --timeout-ms 15000 \
  --property isolation.level=read_committed \
  --property print.key=true \
  --property print.value=true
```

Without `--property isolation.level=read_committed` you can see aborted (duplicate-looking) records. Every future consumer of `payments` (notification, shipping, …) **must** set:

```
mp.messaging.incoming.<channel>.isolation.level=read_committed
```

The automated proof is `PaymentProcessorTest.chaosCrashAbortsThenRetryCommitsExactlyOnePayment`: arm chaos → produce one order → `payments-verify` with `isolation.level=read_committed` counts **exactly one** `orderId`. Real logs from that test:

```
CHAOS armed — next payment TX will abort mid-processing
CHAOS crash mid-processing orderId=ord-chaos-d6d20b22-2a9f-414b-a141-1d5b00b3209a
SRMSG18262: Aborting transaction for producer id kafka-producer-payments-out in channel payments-out.
SRMSG18204: A message sent to channel `orders-in` has been nacked, ignored failure is: chaos crash mid-processing orderId=ord-chaos-d6d20b22-2a9f-414b-a141-1d5b00b3209a.
payment TX orderId=ord-chaos-d6d20b22-2a9f-414b-a141-1d5b00b3209a status=PAID key=orderId
```

Aborted produce is invisible to `read_committed`; the retry commits **one** `PaymentCompleted`.

## Demo — zombie fencing (same `transactional.id`)

Kafka fences a transactional producer when another producer starts with the **same** `transactional.id` (`initTransactions()`). The old instance then throws `ProducerFencedException` on the next TX.

Automated (real exception from the Dev Services broker), `FencingDemoTest`:

```
ZOMBIE-FENCE transactional.id=fence-demo-3b28ec0c-471b-424f-affb-2da51512b164 first producer fenced: org.apache.kafka.common.errors.ProducerFencedException: There is a newer producer with the same transactionalId which fences the current one.
```

Live two-instance demo (same `INSTANCE_ID` → same `transactional.id=payment-tx-local-1`):

```bash
# terminal 1
INSTANCE_ID=local-1 ./mvnw -pl services/payment-service -am quarkus:dev

# terminal 2 — same INSTANCE_ID, different HTTP port
INSTANCE_ID=local-1 ./mvnw -pl services/payment-service -am quarkus:dev \
  -Dquarkus.http.port=18082
```

Place an order. The **first** instance logs `ProducerFencedException` (zombie) and can no longer commit. The second instance owns `payment-tx-local-1`.

**Never** run two replicas with the same `INSTANCE_ID`. Compose uses `INSTANCE_ID=payment-1`; a second replica must be `payment-2`.

## Tests

```bash
./mvnw -pl services/payment-service -am verify
```

Dev Services uses **Strimzi** (Apache Kafka), not Redpanda: Redpanda transactions do not support consume-transform-produce EOS (KIP-447 / `sendOffsetsToTransaction`). Quarkus 3.33.3 Dev Services providers are `redpanda`, `strimzi`, `kafka-native`.

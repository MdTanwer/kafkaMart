# notification-service

HTTP port: `8084` | Prompt: **S4 — consumer groups, fan-out, assignors, lag**

## Kafka concept this service teaches

**Fan-out by consumer group.** Two independent groups subscribe to the same `payments` topic. Kafka delivers **every** `PaymentCompleted` to **each** group. That is not competing consumers (which share partitions *inside* one group); it is two groups, two full copies.

| Channel | Topic | `group.id` | Assignor | Notes |
| --- | --- | --- | --- | --- |
| `payments-email` | `payments` | `notification-email` | **CooperativeStickyAssignor** (incremental) | logs `Email sent` |
| `payments-sms` | `payments` | `notification-sms` | **RangeAssignor** (eager / stop-the-world) | logs `SMS sent`; `max.poll.interval.ms=10000` |
| `shipments-in` | `shipments` | `notification-email` | CooperativeStickyAssignor | logs `tracking link sent` |

`payments` is produced inside a Kafka transaction (S3). All three channels set **`isolation.level=read_committed`**. Ack is **manual**, after the fake send.

## Run

```bash
docker compose up -d
./mvnw -pl services/notification-service -am quarkus:dev
```

Or JVM image:

```bash
docker compose --profile apps up -d --build notification
```

## REST

- `GET /api/notify/lag` — `AdminClient.listConsumerGroupOffsets` + `listOffsets` per partition for both groups
- `POST /api/notify/chaos/slow?ms=N` — sleep N ms on **each** `payments-sms` record

## Demo — fan-out (one payment, two groups)

```bash
curl -sS -X POST http://localhost:8080/api/orders \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: notify-fanout-1' \
  -d '{"userId":"user-ada","items":[{"sku":"SKU-1","quantity":1,"price":12.00}]}'
echo

sleep 3
curl -sS http://localhost:8084/q/metrics | grep notifications_sent_total
curl -sS http://localhost:8084/api/notify/lag | jq .
```

Expect **both** `channel="email"` and `channel="sms"` counters to increment for the same payment.

## Demo — cooperative vs eager rebalance

Start **two** instances (same groups, different HTTP port). Kill the second. Watch revocation lines.

```bash
# terminal 1
./mvnw -pl services/notification-service -am quarkus:dev

# terminal 2
./mvnw -pl services/notification-service -am quarkus:dev -Dquarkus.http.port=18084
```

Wait until both log `REBALANCE assigned`, then Ctrl-C terminal 2.

**Eager (`notification-sms` / RangeAssignor)** — remaining member revokes **all** partitions, then gets a new full assignment (stop-the-world; processing pauses). Captured when a second member joined then left:

```
REBALANCE revoked group=notification-sms assignor=eager-range partitions=[payments-0, payments-1, payments-2, payments-3, payments-4, payments-5] — EAGER: typically ALL currently owned partitions (stop-the-world)
REBALANCE assigned group=notification-sms assignor=eager-range partitions=[payments-0, payments-1, payments-2] — EAGER: full new assignment after stop-the-world revoke
```

**Cooperative (`notification-email` / CooperativeStickyAssignor)** — remaining member **keeps** partitions it still owns. `revoked` is a **subset** (here 0–2; 3–5 stayed hot):

```
REBALANCE revoked group=notification-email assignor=cooperative-sticky partitions=[payments-0, payments-1, payments-2] — COOPERATIVE: empty or a subset (not a stop-the-world revoke of every partition)
REBALANCE assigned group=notification-email assignor=cooperative-sticky partitions=[] — incremental: only newly owned partitions appear here
```

`NotificationServiceTest.secondMemberShowsCooperativeVsEagerRevoke` joins a throwaway consumer to each group and captures those listener logs.

## Demo — slow chaos, lag growth, then group kick

`payments-sms` has `max.poll.interval.ms=10000` and `max.poll.records=1`. Chaos sleeps on the **Kafka poll thread** (`KafkaClientService.runOnPollingThread`). A 12s stall means the member does not `poll()` in time → Kafka **kicks** it out of `notification-sms` → rebalance. (A `@Blocking` worker sleep would **not** kick: SmallRye keeps polling on the client thread.)

```bash
# 1. Slow SMS to 2.5s/record, publish several payments, watch lag
curl -sS -X POST 'http://localhost:8084/api/notify/chaos/slow?ms=2500'
echo
for i in 1 2 3 4 5 6; do
  curl -sS -X POST http://localhost:8080/api/orders \
    -H 'Content-Type: application/json' \
    -H "Idempotency-Key: notify-slow-$i" \
    -d '{"userId":"user-ada","items":[{"sku":"SKU-1","quantity":1,"price":12.00}]}'
  echo
done
sleep 2
curl -sS http://localhost:8084/api/notify/lag | jq '.groups[] | {groupId,totalLag}'

# email lag stays near 0; sms lag grows

# 2. Kick: sleep longer than max.poll.interval.ms
curl -sS -X POST 'http://localhost:8084/api/notify/chaos/slow?ms=12000'
echo
curl -sS -X POST http://localhost:8080/api/orders \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: notify-kick-1' \
  -d '{"userId":"user-ada","items":[{"sku":"SKU-1","quantity":1,"price":12.00}]}'
echo
# wait ~12s — logs show the kick, then a rejoin
curl -sS -X POST 'http://localhost:8084/api/notify/chaos/slow?ms=0'
```

Captured from `NotificationServiceTest.slowChaosGrowsSmsLagThenKickRevokes` (11s poll-thread sleep, `max.poll.interval.ms=10000`):

```
CHAOS slow sleeping 11000ms on payments-sms poll thread
SRMSG18222: Unable to execute consumer revoked re-balance listener for group 'notification-sms':
  CommitFailedException: Offset commit cannot be completed since the consumer is not part of an active group
  for auto partition assignment; it is likely that the consumer was kicked out of the group.
onPartitionsLost for partitions [payments-0, payments-1, payments-2, payments-3, payments-4, payments-5]
REBALANCE revoked group=notification-sms assignor=eager-range partitions=[payments-0, payments-1, payments-2, payments-3, payments-4, payments-5]
```

## Tests

```bash
./mvnw -pl services/notification-service -am verify
```

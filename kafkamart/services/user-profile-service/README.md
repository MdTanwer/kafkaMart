# user-profile-service

HTTP port: `8085` | Prompt: **S5 — schema governance (Avro + Schema Registry + compaction)**

## Kafka concept this service teaches

**The schema is a contract.** `UserProfile` is Avro, not JSON. Confluent Schema Registry stores versions, checks compatibility, and stamps every record with a **schema id**. Compacted topic `users` is a **table**: last value per `user_id` wins; the in-process cache is that table.

## Avro

| File | Fields |
| --- | --- |
| `infra/schemas/UserProfile_v1.avsc` (copied to `src/main/avro/UserProfile.avsc` for codegen) | `user_id`, `name`, `email` |
| `infra/schemas/UserProfile_v2.avsc` | + optional `phone` (`["null","string"]`, default `null`) — **BACKWARD** |

Wire format (Confluent): **`0x00` + 4-byte big-endian schema id + Avro body**.

## Run

```bash
docker compose up -d
./mvnw -pl services/user-profile-service -am quarkus:dev
```

Needs Kafka **and** Schema Registry (`SCHEMA_REGISTRY_URL`, default `http://localhost:8181`). Compose sets `SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL=BACKWARD`.

```bash
docker compose --profile apps up -d --build user-profile
```

## REST

- `POST /api/users` `{userId,name,email}` → Avro `UserProfile` on `users`, **key = user_id**
- `GET /api/users/{id}` → latest profile from the compacted-topic cache (`group.id=user-profile-cache`)

```bash
curl -sS -X POST http://localhost:8085/api/users \
  -H 'Content-Type: application/json' \
  -d '{"userId":"ada","name":"Ada Lovelace","email":"ada@kafkamart.dev"}'
echo
sleep 1
curl -sS http://localhost:8085/api/users/ada
echo
```

## Connector (Confluent serde)

```
mp.messaging.connector.smallrye-kafka.schema.registry.url=${SCHEMA_REGISTRY_URL:http://localhost:8181}
auto.register.schemas=true          # %dev / %test / %prod demo
use.latest.version=true
value.serializer=io.confluent.kafka.serializers.KafkaAvroSerializer
value.deserializer=...KafkaAvroDeserializer
specific.avro.reader=true
```

`%test` omits `schema.registry.url` so **Apicurio Dev Services** (Confluent-compatible `ccompat` API) injects it.

## Schema evolution

v2 only **adds** an optional field with a default. A **running v1 consumer** still deserializes v2 records (writer schema from the id, resolved to the v1 reader schema; `phone` is dropped).

```bash
./scripts/evolve-schema.sh
```

1. Registers v1 on `users-value` (idempotent).
2. **BACKWARD** compatibility check for v2 → `is_compatible: true`.
3. Registers v2.
4. Attempts to register a schema that **deletes `email`**. Under **FULL** (protects still-running v1 readers) the registry returns **HTTP 409**. Compatibility is restored to BACKWARD.

## Magic byte

```bash
./scripts/show-avro-magic-byte.sh
```

Or the automated assertion in `UserProfileTest.avroWireFormatHasMagicByteAndSchemaId`: first byte `0x00`, next four bytes a positive schema id.

## Compaction demo

`users` is created with `cleanup.policy=compact` in `scripts/create-topics.sh`. **Do not** bake `segment.ms` there — the demo alters it live:

```bash
./scripts/demo-compaction.sh
```

Sends 20 updates for `ada`, `kafka-configs --alter` `segment.ms=60000,min.cleanable.dirty.ratio=0.01`, waits for the cleaner, then `kafka-avro-console-consumer --from-beginning`. After compact you should see **one** record for key `ada` (latest name). `GET /api/users/ada` already showed the latest the whole time (cache = table).

## Tests

```bash
./mvnw -pl services/user-profile-service -am verify
```

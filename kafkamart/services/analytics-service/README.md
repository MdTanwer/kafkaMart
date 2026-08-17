# analytics-service

HTTP port: `8088` | Prompt: **S8 — Kafka Connect (converters, SMTs, CDC)**

## Kafka concept this service teaches

Connect is a **cluster**, not a library. Workers share `_connect-configs` / `_connect-offsets` / `_connect-status`. Each connector chooses **converters** (bytes ↔ Connect records) and **SMTs** (Single Message Transforms) before the sink/source plugin runs.

This lab has three connectors:

| Connector | Direction | Lesson |
| --- | --- | --- |
| `jdbc-sink-orders-enriched` | `orders-enriched` → Postgres `enriched_orders` | JDBC **needs a schema**. Worker default is **Avro**. Topic is **JSON** from Streams. Override `JsonConverter` + `schemas.enable=false` + SMT `InferConnectSchema` + `InsertField` `ingested_at`. Upsert `pk.mode=record_key`, `pk.fields=orderId`. |
| `elasticsearch-sink-orders-enriched` | same topic → ES | **Schemaless JSON is OK** (`schema.ignore=true`, `schemas.enable=false`). Same `InsertField` SMT. |
| `debezium-postgres-users` | Postgres `public.users` → `users-cdc` | CDC: `wal_level=logical`. SMT `ExtractNewRecordState` unwraps the Debezium envelope; `RegexRouter` writes **`users-cdc`** (created in `create-topics.sh`, not auto-create). |

## Plugins

```bash
chmod +x scripts/install-connect-plugins.sh scripts/apply-connectors.sh scripts/demo-connect.sh
./scripts/install-connect-plugins.sh
docker compose restart connect
```

Installs into `infra/connect-plugins` (volume on the worker):

- `confluentinc/kafka-connect-jdbc`
- `confluentinc/kafka-connect-elasticsearch`
- `debezium/debezium-connector-postgresql`
- `kafkamart-json-smt` (`InferConnectSchema$Value`)

Compose already sets `CONNECT_PLUGIN_PATH=.../usr/share/kafkamart-plugins`.

## Converter trap

Worker (`docker-compose.yml`):

```
CONNECT_KEY_CONVERTER=org.apache.kafka.connect.json.JsonConverter   schemas.enable=false
CONNECT_VALUE_CONVERTER=io.confluent.connect.avro.AvroConverter     ← default
```

`orders-enriched` is **plain JSON** (S7 `JsonSerde`). A sink that inherits the worker Avro converter will fail. **Every JSON connector in `infra/connectors` overrides** `key.converter` / `value.converter`.

`schemas.enable`:

| Value | Wire | Who |
| --- | --- | --- |
| `true` | `{schema, payload}` Connect JSON | JDBC if you produced envelopes |
| `false` | raw JSON object | our Streams topic; ES `schema.ignore=true`; JDBC only after `InferConnectSchema` |

## SMTs

JDBC chain: **infer schema** (drop nested `items`) → **InsertField** `ingested_at` (wall-clock timestamp column).

CDC chain: **ExtractNewRecordState** (after-image only) → **RegexRouter** `users-cdc`.

## Run

```bash
docker compose up -d
# recreate postgres if it was created before wal_level=logical:
# docker compose up -d --force-recreate postgres
export JAVA_HOME=/data/tools/jdk-25
./mvnw -pl services/analytics-service -am quarkus:dev
./scripts/install-connect-plugins.sh
./scripts/apply-connectors.sh
```

Or:

```bash
curl -sS -X POST http://localhost:8088/api/analytics/connectors/apply
```

## REST

- `GET /api/analytics/connectors` — Connect worker + statuses
- `POST /api/analytics/connectors/apply` — PUT configs from `infra/connectors`
- `GET /api/analytics/orders` — Postgres `enriched_orders` (JDBC sink)
- `GET /api/analytics/orders/{orderId}`
- `GET /api/analytics/search?q=` — Elasticsearch `_search`
- `GET /api/analytics/cdc` — last 50 `users-cdc` records
- `POST /api/analytics/cdc/users` `{userId,name,email}` — upsert `public.users` (CDC source)

## Demos

**JDBC upsert:** produce an enriched order (S5 profile + S1 order + S7). Then:

```bash
docker exec -it kafkamart-postgres psql -U kafkamart -d kafkamart \
  -c 'SELECT "orderId","userName","ingested_at" FROM enriched_orders;'
curl -sS http://localhost:8088/api/analytics/orders
```

Re-send the same `orderId` (Streams re-materializes) → **upsert**, not a second row (`pk.mode=record_key`).

**CDC:**

```bash
curl -sS -X POST http://localhost:8088/api/analytics/cdc/users \
  -H 'Content-Type: application/json' \
  -d '{"userId":"ada","name":"Ada","email":"ada@kafkamart.dev"}'
sleep 2
curl -sS http://localhost:8088/api/analytics/cdc
# update name → another users-cdc record with the new name
```

Or `./scripts/demo-connect.sh`.

## Tests

```bash
export JAVA_HOME=/data/tools/jdk-25
./mvnw -pl services/analytics-service -am verify
```

Connector JSON assertions (JDBC `pk.mode=record_key` / `pk.fields=orderId`, ES schemaless, Debezium unwrap → `users-cdc`) plus health + user upsert.

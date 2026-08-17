# KafkaMart — Agent Constitution (PROMPT 0)

This file is mandatory. Read it before every implementation prompt. Later prompts refine behavior; they never override this constitution unless the user explicitly says so.

## Mission

KafkaMart is a multi-module Spring Boot + Apache Kafka ecommerce demo. Implement **only** the numbered prompt the user pastes next (S1–S11, infra, scripts). Do not invent topics, events, APIs, or services that the current prompt does not specify.

## Prompt cadence

1. Wait for the next prompt. Implement that prompt fully, then stop.
2. Do not skip ahead to later services.
3. If a prompt conflicts with this file, ask before changing the constitution.
4. Keep changes scoped: one service / one infra concern per prompt unless the prompt says otherwise.

## Repository layout

| Path | Role |
|------|------|
| `kafkamart-common/` | Shared DTOs, topic constants, Kafka helpers, test utils |
| `services/order-api-service/` | S1 — HTTP order intake (8080) |
| `services/inventory-service/` | S2 — stock reservation (8081) |
| `services/payment-service/` | S3 — payment capture (8082) |
| `services/notification-service/` | S4 — customer notifications (8084) |
| `services/user-profile-service/` | S5 — Avro user profiles + Schema Registry (8085) |
| `services/fraud-detection-service/` | S6 — Kafka Streams fraud (8086) |
| `services/order-enrichment-service/` | S7 — join/enrich orders (8087) |
| `services/analytics-service/` | S8 — Connect/analytics sink (8088) |
| `services/audit-dlq-service/` | S9 — DLQ / audit (8089) |
| `services/ops-monitor-service/` | S10 — ops / lag / health (8090) |
| `services/shipping-service/` | S11 — shipping (8091) |
| `infra/connectors/` | Kafka Connect JSON (S8) |
| `infra/schemas/` | Avro `.avsc` v1/v2 (S5) |
| `infra/prometheus/`, `infra/grafana/` | Metrics dashboards |
| `scripts/` | `create-topics.sh`, `chaos/`, `load/` |

Port **8083** is reserved for Schema Registry. Do not bind an app there.

## Stack (do not change unless a prompt says so)

- Java 17, Maven multi-module, Spring Boot 3.3
- `spring-kafka`; Kafka Streams only where the service POM already includes it
- JSON for domain events unless the prompt requires Avro (S5 / enrichment)
- Confluent Schema Registry + Avro for user-profile contracts
- Docker Compose: Kafka (KRaft), Schema Registry, Kafka Connect, Prometheus, Grafana
- Actuator: `health`, `info`, `prometheus`, `metrics`

## Kafka contracts

Until a later prompt names topics, use constants in `kafkamart-common` — do not hard-code topic strings in services.

Producer defaults (already in each `application.yml`):

- `acks=all`
- `enable.idempotence=true`
- JSON type headers on
- `isolation.level=read_committed` on consumers
- listener `ack-mode: record`
- Streams `processing.guarantee=exactly_once_v2` where Streams is used

Every published record should carry identity headers (service id, event type, event id, correlation / causation). Implement this in `EventPublisher` when that prompt arrives; services must not copy-paste producer header logic.

Consumer failures go to a DLQ via the shared `KafkaErrorHandlerConfig`. Do not swallow exceptions in listeners.

Schema evolution (S5): **BACKWARD** compatibility only. Add optional fields with defaults. Never rename/remove fields or change types in place. Version Avro under `infra/schemas/<name>/v1` and `v2`.

## Code style

- Match existing packages: `com.kafkamart.<service>`
- Plain Java (getters/setters). No Lombok unless a prompt requires it.
- Business logic in services; DTOs/events in `kafkamart-common`
- New endpoints: validation + actuator health; no extra frameworks
- Tests live under each module’s `src/test/java` when the prompt asks for them
- Do not commit secrets. Keep credentials in `.env` only.

## Infra / scripts

- Topics are created by `scripts/create-topics.sh`, not by apps at runtime.
- Connect configs are JSON files in `infra/connectors/` (posted to Connect, not compiled into apps).
- Chaos and load scripts stay under `scripts/chaos/` and `scripts/load/`.
- Compose service names and env vars must stay aligned with `.env`.

## Done when

The current prompt’s files compile against the parent POM, follow this constitution, and do not implement the next prompt.

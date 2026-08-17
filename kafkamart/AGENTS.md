# KAFKAMART — AGENT CONSTITUTION (NON-NEGOTIABLE)

Read this file before every implementation prompt. Later prompts refine behavior; they never override this constitution unless the user explicitly says so. Implement **only** the numbered prompt the user pastes next.

## Stack
- Java 21, Maven, latest Quarkus 3.x LTS, JVM mode (native = optional stretch)
- Kafka: SmallRye Reactive Messaging (quarkus-smallrye-reactive-messaging-kafka)
- Streams: quarkus-kafka-streams | Avro: quarkus-confluent-registry-avro
- DB: quarkus-hibernate-orm-panache + quarkus-jdbc-postgresql
- Tests: JUnit5, @QuarkusTest, Dev Services (Kafka), Testcontainers for e2e

## EVERY service MUST have (Definition of Done):
1. Health: /q/health/live + /q/health/ready (Kafka readiness MUST fail if broker down)
2. Metrics: quarkus-micrometer-registry-prometheus at /q/metrics + custom business counters
3. OpenAPI: quarkus-smallrye-openapi (/q/openapi)
4. JSON console logs in %prod profile (quarkus-log-json), pretty in %dev
5. ALL infra config from env vars with localhost defaults:
   kafka.bootstrap.servers=${KAFKA_BOOTSTRAP_SERVERS:localhost:9092,localhost:9094,localhost:9096}
   Never hardcode brokers, topics, credentials.
6. Bean Validation (quarkus-hibernate-validator) on every REST input
7. Graceful shutdown: quarkus.shutdown.timeout=30S. Never System.exit().
8. Dockerfile.jvm (multi-stage) in src/main/docker + docker-compose service entry
9. README.md: run commands + WHICH Kafka concept this service teaches + how to demo it
10. Tests green: ./mvnw verify. No TODO/FIXME left. Idempotent consumers ALWAYS.

## Conventions
- Group: com.kafkamart | Package: com.kafkamart.<servicename>
- DTOs are Java records in kafkamart-common (JSON via Jackson, except UserProfile = Avro)
- Every event carries: eventId (UUID), occurredAt (Instant), traceId (MDC-propagated)
- Producers: acks=all + enable.idempotence=true UNLESS the exercise says otherwise
- Topics are created ONLY via scripts/create-topics.sh (auto.create.topics.enable=false)

## Ports
order-api 8080 | inventory 8081 | payment 8082 | notification 8084 | user-profile 8085
fraud 8086 | enrichment 8087 | analytics 8088 | audit-dlq 8089 | ops-monitor 8090 | shipping 8091
Kafka host ports 9092/9094/9096 (internal 19092) | Schema Registry 8181 | Kafka-UI 9000
Connect 8083 | Postgres 5432 | Elasticsearch 9200 | Prometheus 9090 | Grafana 3000

## When stuck
- SmallRye Kafka connector config reference: smallrye.io/smallrye-reactive-messaging (Kafka connector attributes)
- Quarkus guides: quarkus.io/guides/kafka, /kafka-streams, /confluent-registry-avro, /kafka-dev-services
- Unknown mp.messaging channel attributes PASS THROUGH to the Kafka client — use this for any producer/consumer tuning.

## Prompt cadence
1. Wait for the next prompt. Implement that prompt fully, then stop.
2. Do not skip ahead to later services.
3. If a prompt conflicts with this file, ask before changing the constitution.
4. Keep changes scoped: one service / one infra concern per prompt unless the prompt says otherwise.

## Repository layout

| Path | Role |
|------|------|
| `kafkamart-common/` | Shared records, topic constants, test utils |
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

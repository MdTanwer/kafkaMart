# order-enrichment-service

HTTP port: `8087` | Prompt: **S7**

## Kafka concept this service teaches

KStream-KTable join to enrich orders with user profile

## Run

Infra (from repo root):

```bash
docker compose up -d kafka-1 kafka-2 kafka-3
./mvnw -pl services/order-enrichment-service -am quarkus:dev
```

Or JVM image:

```bash
docker compose up -d --build enrichment
```

## Demo

1. Check liveness: `curl -s localhost:8087/q/health/live`
2. Check readiness (fails if Kafka is down): `curl -s localhost:8087/q/health/ready`
3. Metrics: `curl -s localhost:8087/q/metrics | grep kafkamart`
4. OpenAPI: `curl -s localhost:8087/q/openapi`
5. Produce a profile then an order; the enriched output includes loyalty tier.

## Config

All broker/topic/credential values come from environment variables. Defaults:

`KAFKA_BOOTSTRAP_SERVERS=localhost:9092,localhost:9094,localhost:9096`

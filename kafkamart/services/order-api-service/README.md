# order-api-service

HTTP port: `8080` | Prompt: **S1**

## Kafka concept this service teaches

HTTP ingress producing to Kafka (record keys, acks=all, idempotent producer)

## Run

Infra (from repo root):

```bash
docker compose up -d kafka-1 kafka-2 kafka-3
./mvnw -pl services/order-api-service -am quarkus:dev
```

Or JVM image:

```bash
docker compose up -d --build order-api
```

## Demo

1. Check liveness: `curl -s localhost:8080/q/health/live`
2. Check readiness (fails if Kafka is down): `curl -s localhost:8080/q/health/ready`
3. Metrics: `curl -s localhost:8080/q/metrics | grep kafkamart`
4. OpenAPI: `curl -s localhost:8080/q/openapi`
5. POST an order, then watch the event in Kafka-UI (localhost:9000) on the orders topic.

## Config

All broker/topic/credential values come from environment variables. Defaults:

`KAFKA_BOOTSTRAP_SERVERS=localhost:9092,localhost:9094,localhost:9096`

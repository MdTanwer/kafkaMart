# notification-service

HTTP port: `8084` | Prompt: **S4**

## Kafka concept this service teaches

Fan-out consumers and at-least-once delivery with idempotency

## Run

Infra (from repo root):

```bash
docker compose up -d kafka-1 kafka-2 kafka-3
./mvnw -pl services/notification-service -am quarkus:dev
```

Or JVM image:

```bash
docker compose up -d --build notification
```

## Demo

1. Check liveness: `curl -s localhost:8084/q/health/live`
2. Check readiness (fails if Kafka is down): `curl -s localhost:8084/q/health/ready`
3. Metrics: `curl -s localhost:8084/q/metrics | grep kafkamart`
4. OpenAPI: `curl -s localhost:8084/q/openapi`
5. Place an order and confirm a notification is recorded once per eventId.

## Config

All broker/topic/credential values come from environment variables. Defaults:

`KAFKA_BOOTSTRAP_SERVERS=localhost:9092,localhost:9094,localhost:9096`

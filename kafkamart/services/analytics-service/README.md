# analytics-service

HTTP port: `8088` | Prompt: **S8**

## Kafka concept this service teaches

Kafka Connect sink / analytics pipeline

## Run

Infra (from repo root):

```bash
docker compose up -d kafka-1 kafka-2 kafka-3
./mvnw -pl services/analytics-service -am quarkus:dev
```

Or JVM image:

```bash
docker compose up -d --build analytics
```

## Demo

1. Check liveness: `curl -s localhost:8088/q/health/live`
2. Check readiness (fails if Kafka is down): `curl -s localhost:8088/q/health/ready`
3. Metrics: `curl -s localhost:8088/q/metrics | grep kafkamart`
4. OpenAPI: `curl -s localhost:8088/q/openapi`
5. Apply a connector JSON from infra/connectors and query the sink.

## Config

All broker/topic/credential values come from environment variables. Defaults:

`KAFKA_BOOTSTRAP_SERVERS=localhost:9092,localhost:9094,localhost:9096`

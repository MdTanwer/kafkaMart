# audit-dlq-service

HTTP port: `8089` | Prompt: **S9**

## Kafka concept this service teaches

Dead letter queues, poison pills, and retry headers

## Run

Infra (from repo root):

```bash
docker compose up -d kafka-1 kafka-2 kafka-3
./mvnw -pl services/audit-dlq-service -am quarkus:dev
```

Or JVM image:

```bash
docker compose up -d --build audit-dlq
```

## Demo

1. Check liveness: `curl -s localhost:8089/q/health/live`
2. Check readiness (fails if Kafka is down): `curl -s localhost:8089/q/health/ready`
3. Metrics: `curl -s localhost:8089/q/metrics | grep kafkamart`
4. OpenAPI: `curl -s localhost:8089/q/openapi`
5. Publish a malformed event and confirm it lands on the DLQ with error metadata.

## Config

All broker/topic/credential values come from environment variables. Defaults:

`KAFKA_BOOTSTRAP_SERVERS=localhost:9092,localhost:9094,localhost:9096`

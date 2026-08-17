# inventory-service

HTTP port: `8081` | Prompt: **S2**

## Kafka concept this service teaches

Consumer groups, partitions, and idempotent reservation

## Run

Infra (from repo root):

```bash
docker compose up -d kafka-1 kafka-2 kafka-3
./mvnw -pl services/inventory-service -am quarkus:dev
```

Or JVM image:

```bash
docker compose up -d --build inventory
```

## Demo

1. Check liveness: `curl -s localhost:8081/q/health/live`
2. Check readiness (fails if Kafka is down): `curl -s localhost:8081/q/health/ready`
3. Metrics: `curl -s localhost:8081/q/metrics | grep kafkamart`
4. OpenAPI: `curl -s localhost:8081/q/openapi`
5. Publish two identical order events; stock must decrement once.

## Config

All broker/topic/credential values come from environment variables. Defaults:

`KAFKA_BOOTSTRAP_SERVERS=localhost:9092,localhost:9094,localhost:9096`

# payment-service

HTTP port: `8082` | Prompt: **S3**

## Kafka concept this service teaches

Transactional consume-transform-produce and payment outcomes

## Run

Infra (from repo root):

```bash
docker compose up -d kafka-1 kafka-2 kafka-3
./mvnw -pl services/payment-service -am quarkus:dev
```

Or JVM image:

```bash
docker compose up -d --build payment
```

## Demo

1. Check liveness: `curl -s localhost:8082/q/health/live`
2. Check readiness (fails if Kafka is down): `curl -s localhost:8082/q/health/ready`
3. Metrics: `curl -s localhost:8082/q/metrics | grep kafkamart`
4. OpenAPI: `curl -s localhost:8082/q/openapi`
5. Pay a reserved order and observe paid vs rejected events.

## Config

All broker/topic/credential values come from environment variables. Defaults:

`KAFKA_BOOTSTRAP_SERVERS=localhost:9092,localhost:9094,localhost:9096`

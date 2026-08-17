# shipping-service

HTTP port: `8091` | Prompt: **S11**

## Kafka concept this service teaches

Downstream shipping events after payment (choreography)

## Run

Infra (from repo root):

```bash
docker compose up -d kafka-1 kafka-2 kafka-3
./mvnw -pl services/shipping-service -am quarkus:dev
```

Or JVM image:

```bash
docker compose up -d --build shipping
```

## Demo

1. Check liveness: `curl -s localhost:8091/q/health/live`
2. Check readiness (fails if Kafka is down): `curl -s localhost:8091/q/health/ready`
3. Metrics: `curl -s localhost:8091/q/metrics | grep kafkamart`
4. OpenAPI: `curl -s localhost:8091/q/openapi`
5. Complete payment and observe a shipping event keyed by orderId.

## Config

All broker/topic/credential values come from environment variables. Defaults:

`KAFKA_BOOTSTRAP_SERVERS=localhost:9092,localhost:9094,localhost:9096`

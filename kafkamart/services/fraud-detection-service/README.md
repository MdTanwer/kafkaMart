# fraud-detection-service

HTTP port: `8086` | Prompt: **S6**

## Kafka concept this service teaches

Kafka Streams windowed aggregation for fraud signals

## Run

Infra (from repo root):

```bash
docker compose up -d kafka-1 kafka-2 kafka-3
./mvnw -pl services/fraud-detection-service -am quarkus:dev
```

Or JVM image:

```bash
docker compose up -d --build fraud
```

## Demo

1. Check liveness: `curl -s localhost:8086/q/health/live`
2. Check readiness (fails if Kafka is down): `curl -s localhost:8086/q/health/ready`
3. Metrics: `curl -s localhost:8086/q/metrics | grep kafkamart`
4. OpenAPI: `curl -s localhost:8086/q/openapi`
5. Burst orders for one user and watch a fraud flag event.

## Config

All broker/topic/credential values come from environment variables. Defaults:

`KAFKA_BOOTSTRAP_SERVERS=localhost:9092,localhost:9094,localhost:9096`

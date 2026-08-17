# ops-monitor-service

HTTP port: `8090` | Prompt: **S10**

## Kafka concept this service teaches

AdminClient: consumer lag, topic metadata, cluster health

## Run

Infra (from repo root):

```bash
docker compose up -d kafka-1 kafka-2 kafka-3
./mvnw -pl services/ops-monitor-service -am quarkus:dev
```

Or JVM image:

```bash
docker compose up -d --build ops-monitor
```

## Demo

1. Check liveness: `curl -s localhost:8090/q/health/live`
2. Check readiness (fails if Kafka is down): `curl -s localhost:8090/q/health/ready`
3. Metrics: `curl -s localhost:8090/q/metrics | grep kafkamart`
4. OpenAPI: `curl -s localhost:8090/q/openapi`
5. Hit ops endpoints while a consumer is paused and watch lag rise.

## Config

All broker/topic/credential values come from environment variables. Defaults:

`KAFKA_BOOTSTRAP_SERVERS=localhost:9092,localhost:9094,localhost:9096`

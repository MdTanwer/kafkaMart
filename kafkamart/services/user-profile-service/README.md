# user-profile-service

HTTP port: `8085` | Prompt: **S5**

## Kafka concept this service teaches

Avro + Confluent Schema Registry with BACKWARD compatibility

## Run

Infra (from repo root):

```bash
docker compose up -d kafka-1 kafka-2 kafka-3
./mvnw -pl services/user-profile-service -am quarkus:dev
```

Or JVM image:

```bash
docker compose up -d --build user-profile
```

## Demo

1. Check liveness: `curl -s localhost:8085/q/health/live`
2. Check readiness (fails if Kafka is down): `curl -s localhost:8085/q/health/ready`
3. Metrics: `curl -s localhost:8085/q/metrics | grep kafkamart`
4. OpenAPI: `curl -s localhost:8085/q/openapi`
5. Publish v1 then v2 UserProfile; old consumers still read v2.

## Config

All broker/topic/credential values come from environment variables. Defaults:

`KAFKA_BOOTSTRAP_SERVERS=localhost:9092,localhost:9094,localhost:9096`

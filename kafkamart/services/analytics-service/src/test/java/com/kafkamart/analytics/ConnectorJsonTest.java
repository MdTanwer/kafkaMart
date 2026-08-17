package com.kafkamart.analytics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class ConnectorJsonTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Path DIR = Path.of("..", "..", "infra", "connectors");

    @Test
    void jdbcSinkUpsertsByOrderIdRecordKey() throws Exception {
        JsonNode cfg = config("jdbc-sink-orders-enriched.json");
        assertEquals(
                "io.confluent.connect.jdbc.JdbcSinkConnector",
                cfg.path("connector.class").asText());
        assertEquals("orders-enriched", cfg.path("topics").asText());
        assertEquals("enriched_orders", cfg.path("table.name.format").asText());
        assertEquals("upsert", cfg.path("insert.mode").asText());
        assertEquals("record_key", cfg.path("pk.mode").asText());
        assertEquals("orderId", cfg.path("pk.fields").asText());
        assertEquals("false", cfg.path("value.converter.schemas.enable").asText());
        assertTrue(cfg.path("transforms").asText().contains("infer"));
        assertEquals(
                "com.kafkamart.connect.InferConnectSchema$Value",
                cfg.path("transforms.infer.type").asText());
        assertEquals(
                "org.apache.kafka.connect.transforms.InsertField$Value",
                cfg.path("transforms.insertTs.type").asText());
        assertEquals("ingested_at", cfg.path("transforms.insertTs.timestamp.field").asText());
    }

    @Test
    void elasticsearchSinkIsSchemalessJson() throws Exception {
        JsonNode cfg = config("elasticsearch-sink-orders-enriched.json");
        assertEquals(
                "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
                cfg.path("connector.class").asText());
        assertEquals("true", cfg.path("schema.ignore").asText());
        assertEquals("false", cfg.path("value.converter.schemas.enable").asText());
        assertEquals(
                "org.apache.kafka.connect.json.JsonConverter",
                cfg.path("value.converter").asText());
    }

    @Test
    void debeziumCdcUnwrapsAndReroutesToUsersCdc() throws Exception {
        JsonNode cfg = config("debezium-postgres-users.json");
        assertEquals(
                "io.debezium.connector.postgresql.PostgresConnector",
                cfg.path("connector.class").asText());
        assertEquals("public.users", cfg.path("table.include.list").asText());
        assertEquals(
                "io.debezium.transforms.ExtractNewRecordState",
                cfg.path("transforms.unwrap.type").asText());
        assertEquals("users-cdc", cfg.path("transforms.reroute.replacement").asText());
        assertEquals("false", cfg.path("value.converter.schemas.enable").asText());
    }

    private static JsonNode config(String file) throws Exception {
        Path path = DIR.resolve(file);
        assertTrue(Files.isRegularFile(path), "missing " + path.toAbsolutePath());
        JsonNode root = MAPPER.readTree(path.toFile());
        assertTrue(root.path("name").asText().length() > 0);
        return root.path("config");
    }
}

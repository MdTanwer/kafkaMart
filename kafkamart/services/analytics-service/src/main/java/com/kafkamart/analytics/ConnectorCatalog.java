package com.kafkamart.analytics;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class ConnectorCatalog {
    private final ObjectMapper mapper;
    private final Path dir;

    public ConnectorCatalog(
            ObjectMapper mapper, @ConfigProperty(name = "analytics.connectors.dir") String dir) {
        this.mapper = mapper;
        this.dir = Path.of(dir);
    }

    public List<JsonNode> loadAll() {
        List<JsonNode> out = new ArrayList<>();
        if (!Files.isDirectory(dir)) {
            return out;
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.json")) {
            for (Path file : stream) {
                out.add(mapper.readTree(file.toFile()));
            }
        } catch (IOException failure) {
            throw new IllegalStateException("failed to read connectors from " + dir, failure);
        }
        return out;
    }

    public Path dir() {
        return dir;
    }
}

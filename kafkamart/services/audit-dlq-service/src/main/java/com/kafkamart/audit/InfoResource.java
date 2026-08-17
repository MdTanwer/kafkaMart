package com.kafkamart.audit;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.util.Map;

@Path("/info")
public class InfoResource {
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, String> info() {
        return Map.of(
                "service", "audit-dlq-service",
                "prompt", "S9");
    }
}

package com.kafkamart.fraud;

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
                "service", "fraud-detection-service",
                "prompt", "S6");
    }
}

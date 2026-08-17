package com.kafkamart.fraud;

import com.kafkamart.common.event.FraudAlert;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.util.List;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

@Path("/api/fraud")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "fraud", description = "Kafka Streams fraud alerts (high-value + velocity)")
public class FraudResource {
    @Inject FraudAlertBuffer buffer;

    @GET
    @Path("/alerts")
    @Operation(summary = "Last 100 FraudAlert records consumed from topic fraud-alerts")
    public List<FraudAlert> alerts() {
        return buffer.snapshot();
    }
}

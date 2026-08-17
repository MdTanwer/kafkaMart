package com.kafkamart.notification;

import com.kafkamart.notification.api.LagReport;
import jakarta.inject.Inject;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import java.util.Map;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

@Path("/api/notify")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "notify", description = "Fan-out lag and sms slow-chaos")
public class NotifyResource {
    @Inject LagService lag;
    @Inject SlowChaos chaos;

    @GET
    @Path("/lag")
    @Operation(summary = "Per-partition lag for notification-email and notification-sms")
    public LagReport lag() {
        return lag.snapshot();
    }

    @POST
    @Path("/chaos/slow")
    @Operation(summary = "Sleep N ms per payments-sms record (max.poll.interval.ms kick-out demo)")
    public Map<String, Object> slow(
            @QueryParam("ms") @DefaultValue("0") @Min(0) @Max(120000) int ms) {
        chaos.setSleepMs(ms);
        return Map.of("sleepMs", chaos.sleepMs(), "channel", "payments-sms");
    }
}

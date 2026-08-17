package com.kafkamart.orderapi;

import com.kafkamart.orderapi.api.CreateOrderRequest;
import com.kafkamart.orderapi.api.OrderAccepted;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.concurrent.CompletionStage;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

@Path("/api/orders")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "orders", description = "HTTP ingress that produces OrderCreated events")
public class OrderResource {
    @Inject OrderService orders;

    @POST
    @Operation(summary = "Accept an order and emit OrderCreated to Kafka (acks=all)")
    public CompletionStage<Response> create(
            @Valid @NotNull CreateOrderRequest request,
            @HeaderParam("Idempotency-Key") String idempotencyKey) {
        return orders.place(request, idempotencyKey)
                .thenApply(orderId -> Response.accepted(new OrderAccepted(orderId)).build());
    }

    @GET
    @Path("/simulate-load")
    @Operation(summary = "Fire N orders across 3 fake users for load / partitioner demos")
    public CompletionStage<Response> simulateLoad(
            @QueryParam("count") @DefaultValue("10") int count) {
        if (count < 1) {
            return java.util.concurrent.CompletableFuture.completedFuture(
                    Response.status(Response.Status.BAD_REQUEST)
                            .entity("{\"error\":\"count must be >= 1\"}")
                            .build());
        }
        return orders.simulateLoad(count)
                .thenApply(
                        orderIds ->
                                Response.accepted(
                                                new OrderAccepted.LoadAccepted(
                                                        orderIds.size(), orderIds))
                                        .build());
    }
}

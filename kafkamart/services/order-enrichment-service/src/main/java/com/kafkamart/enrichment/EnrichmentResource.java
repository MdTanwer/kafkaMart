package com.kafkamart.enrichment;

import com.kafkamart.common.event.EnrichedOrder;
import jakarta.inject.Inject;
import jakarta.validation.constraints.NotBlank;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

@Path("/api/enrichment")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "enrichment", description = "KStream-KTable join + Interactive Queries")
public class EnrichmentResource {
    @Inject InteractiveQueries queries;

    @GET
    @Path("/orders/{orderId}")
    @Operation(
            summary =
                    "Interactive Query of orders-store. 200 EnrichedOrder, or 404 with metadata.host")
    public Response order(@PathParam("orderId") @NotBlank String orderId) {
        OrderQueryResult result = queries.find(orderId);
        if (result.found()) {
            EnrichedOrder order = result.order();
            if (order == null) {
                throw new NotFoundException(orderId);
            }
            return Response.ok(order).build();
        }
        return Response.status(Response.Status.NOT_FOUND).entity(result.missBody(orderId)).build();
    }
}

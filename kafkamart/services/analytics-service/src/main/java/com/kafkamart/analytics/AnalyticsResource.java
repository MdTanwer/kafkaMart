package com.kafkamart.analytics;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kafkamart.analytics.api.UpsertUserRequest;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/api/analytics")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "analytics", description = "Kafka Connect control plane + JDBC/ES/CDC queries")
public class AnalyticsResource {
    private static final Logger LOG = LoggerFactory.getLogger(AnalyticsResource.class);

    @Inject ConnectorCatalog catalog;
    @Inject CdcBuffer cdc;
    @Inject ServiceMetrics metrics;
    @Inject ObjectMapper mapper;

    @RestClient ConnectClient connect;
    @RestClient ElasticsearchClient elasticsearch;

    @GET
    @Path("/connectors")
    @Operation(summary = "Connect REST: connector names + status (and worker converters)")
    public Response connectors() {
        try {
            ObjectNode body = mapper.createObjectNode();
            body.set("worker", connect.worker());
            body.set("connectors", connect.connectors());
            List<JsonNode> statuses = new ArrayList<>();
            if (connect.connectors().isArray()) {
                for (JsonNode name : connect.connectors()) {
                    statuses.add(connect.status(name.asText()));
                }
            }
            body.set("status", mapper.valueToTree(statuses));
            return Response.ok(body).build();
        } catch (RuntimeException failure) {
            LOG.warn("Connect unreachable: {}", failure.getMessage());
            return Response.status(Response.Status.BAD_GATEWAY)
                    .entity(
                            Map.of(
                                    "error",
                                    "connect unreachable",
                                    "detail",
                                    String.valueOf(failure)))
                    .build();
        }
    }

    @POST
    @Path("/connectors/apply")
    @Operation(summary = "PUT each infra/connectors/*.json config to Connect")
    public Response apply() {
        List<Map<String, Object>> results = new ArrayList<>();
        for (JsonNode file : catalog.loadAll()) {
            String name = file.path("name").asText();
            JsonNode config = file.path("config");
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("name", name);
            try {
                JsonNode saved = connect.upsert(name, config);
                row.put("ok", true);
                row.put("config", saved);
                metrics.produced();
            } catch (RuntimeException failure) {
                row.put("ok", false);
                row.put("error", String.valueOf(failure.getMessage()));
            }
            results.add(row);
        }
        return Response.accepted(Map.of("dir", catalog.dir().toString(), "results", results))
                .build();
    }

    @GET
    @Path("/orders")
    @Operation(summary = "Rows from Postgres enriched_orders (JDBC sink)")
    public List<EnrichedOrderRow> orders() {
        return EnrichedOrderRow.listAll();
    }

    @GET
    @Path("/orders/{orderId}")
    public Response order(@PathParam("orderId") @NotBlank String orderId) {
        EnrichedOrderRow row = EnrichedOrderRow.findById(orderId);
        if (row == null) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(Map.of("orderId", orderId, "error", "not in enriched_orders"))
                    .build();
        }
        return Response.ok(row).build();
    }

    @GET
    @Path("/search")
    @Operation(summary = "Elasticsearch _search (schemaless JSON sink)")
    public Response search(@QueryParam("q") String q) {
        try {
            return Response.ok(elasticsearch.search(q == null || q.isBlank() ? "*" : q)).build();
        } catch (RuntimeException failure) {
            return Response.status(Response.Status.BAD_GATEWAY)
                    .entity(
                            Map.of(
                                    "error",
                                    "elasticsearch unreachable",
                                    "detail",
                                    String.valueOf(failure)))
                    .build();
        }
    }

    @GET
    @Path("/cdc")
    @Operation(summary = "Last 50 users-cdc records (Debezium ExtractNewRecordState)")
    public List<Map<String, Object>> cdc() {
        return cdc.snapshot();
    }

    @POST
    @Path("/cdc/users")
    @Transactional
    @Operation(summary = "Upsert public.users — Debezium should emit to users-cdc")
    public UserRow upsertUser(@Valid @NotNull UpsertUserRequest request) {
        UserRow row = UserRow.findById(request.userId());
        if (row == null) {
            row = new UserRow();
            row.userId = request.userId();
        }
        row.name = request.name();
        row.email = request.email();
        row.persist();
        metrics.produced();
        return row;
    }
}

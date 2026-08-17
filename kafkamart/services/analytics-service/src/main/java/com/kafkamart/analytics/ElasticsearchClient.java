package com.kafkamart.analytics;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

@RegisterRestClient(configKey = "elasticsearch")
@Path("/")
@Produces(MediaType.APPLICATION_JSON)
public interface ElasticsearchClient {
    @GET
    @Path("/_search")
    JsonNode search(@QueryParam("q") String query);
}

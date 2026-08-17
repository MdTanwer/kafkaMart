package com.kafkamart.analytics;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

@RegisterRestClient(configKey = "connect")
@Path("/")
@Produces(MediaType.APPLICATION_JSON)
public interface ConnectClient {
    @GET
    JsonNode worker();

    @GET
    @Path("/connectors")
    JsonNode connectors();

    @GET
    @Path("/connectors/{name}/status")
    JsonNode status(@PathParam("name") String name);

    @PUT
    @Path("/connectors/{name}/config")
    JsonNode upsert(@PathParam("name") String name, JsonNode config);
}

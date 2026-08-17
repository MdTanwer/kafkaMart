package com.kafkamart.userprofile;

import com.kafkamart.userprofile.api.UpsertUserRequest;
import com.kafkamart.userprofile.api.UserProfileView;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

@Path("/api/users")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "users", description = "Avro user profiles on compacted topic users")
public class UserResource {
    @Inject UserProfileProducer producer;
    @Inject UserProfileCache cache;

    @POST
    @Operation(summary = "Upsert a user profile and emit Avro UserProfile (key=user_id)")
    public Response upsert(@Valid @NotNull UpsertUserRequest request) {
        producer.upsert(request.userId(), request.name(), request.email());
        return Response.accepted(
                        new UserProfileView(request.userId(), request.name(), request.email()))
                .build();
    }

    @GET
    @Path("/{id}")
    @Operation(summary = "Latest profile from the compacted-topic cache")
    public UserProfileView get(@PathParam("id") @NotBlank String id) {
        return cache.get(id)
                .map(UserProfileView::from)
                .orElseThrow(() -> new NotFoundException("unknown user " + id));
    }
}

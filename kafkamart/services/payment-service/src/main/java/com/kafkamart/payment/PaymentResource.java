package com.kafkamart.payment;

import com.kafkamart.payment.api.PaymentConfig;
import jakarta.inject.Inject;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.Map;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

@Path("/api/payments")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "payments", description = "EOS config and chaos crash for the transactions demo")
public class PaymentResource {
    static final String EOS_PATTERN =
            "KafkaTransactions.withTransactionAndAck (SmallRye 4.33 consume-transform-produce)";
    static final String EXACTLY_ONCE_NOTE =
            "not a connector attribute in SmallRye 4.33; equivalent is "
                    + EOS_PATTERN
                    + " — do not use a plain @Incoming/@Outgoing processor";
    static final String TRANSACTIONAL_NOTE =
            "not a connector attribute in SmallRye 4.33; transactional.id on payments-out enables"
                    + " the transactional producer";

    @Inject ChaosSwitch chaos;

    @ConfigProperty(name = "mp.messaging.incoming.orders-in.commit-strategy")
    String commitStrategy;

    @ConfigProperty(name = "mp.messaging.incoming.orders-in.failure-strategy")
    String failureStrategy;

    @ConfigProperty(name = "mp.messaging.incoming.orders-in.enable.auto.commit")
    boolean enableAutoCommit;

    @ConfigProperty(name = "mp.messaging.outgoing.payments-out.transactional.id")
    String transactionalId;

    @ConfigProperty(name = "INSTANCE_ID", defaultValue = "local-1")
    String instanceId;

    @GET
    @Path("/config")
    @Operation(summary = "Effective Kafka EOS config (demo)")
    public PaymentConfig config() {
        return new PaymentConfig(
                EXACTLY_ONCE_NOTE,
                commitStrategy,
                failureStrategy,
                enableAutoCommit,
                TRANSACTIONAL_NOTE,
                transactionalId,
                EOS_PATTERN,
                "read_committed",
                instanceId,
                chaos.enabled());
    }

    @POST
    @Path("/chaos/crash")
    @Operation(summary = "Arm a mid-TX crash on the next order (dev/test only)")
    public Response crash() {
        if (!chaos.enabled()) {
            throw new ForbiddenException("chaos is disabled in this profile");
        }
        chaos.arm();
        return Response.accepted(
                        Map.of(
                                "armed",
                                true,
                                "message",
                                "next OrderCreated will abort the payment TX after produce"))
                .build();
    }
}

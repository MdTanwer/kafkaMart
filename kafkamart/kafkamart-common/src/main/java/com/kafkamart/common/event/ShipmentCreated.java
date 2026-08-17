package com.kafkamart.common.event;

import com.kafkamart.common.trace.TraceId;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;
import java.util.UUID;

public record ShipmentCreated(
        @NotNull UUID eventId,
        @NotNull Instant occurredAt,
        @NotBlank String traceId,
        @NotBlank String shipmentId,
        @NotBlank String orderId,
        @NotBlank String userId,
        @NotBlank String status) {

    public static ShipmentCreated of(
            String shipmentId, String orderId, String userId, String status) {
        return new ShipmentCreated(
                UUID.randomUUID(),
                Instant.now(),
                TraceId.currentOrNew(),
                shipmentId,
                orderId,
                userId,
                status);
    }
}

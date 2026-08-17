package com.kafkamart.common.event;

import com.kafkamart.common.trace.TraceId;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;
import java.util.UUID;

public record FraudAlert(
        @NotNull UUID eventId,
        @NotNull Instant occurredAt,
        @NotBlank String traceId,
        @NotBlank String orderId,
        @NotBlank String userId,
        @NotBlank String reason,
        double score) {

    public static FraudAlert of(String orderId, String userId, String reason, double score) {
        return new FraudAlert(
                UUID.randomUUID(),
                Instant.now(),
                TraceId.currentOrNew(),
                orderId,
                userId,
                reason,
                score);
    }
}

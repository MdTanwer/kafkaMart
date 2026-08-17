package com.kafkamart.common.event;

import com.kafkamart.common.trace.TraceId;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record PaymentCompleted(
        @NotNull UUID eventId,
        @NotNull Instant occurredAt,
        @NotBlank String traceId,
        @NotBlank String orderId,
        @NotBlank String userId,
        @NotNull BigDecimal amount,
        @NotNull PaymentStatus status,
        @NotBlank String transactionId) {

    public static PaymentCompleted of(
            String orderId,
            String userId,
            BigDecimal amount,
            PaymentStatus status,
            String transactionId) {
        return new PaymentCompleted(
                UUID.randomUUID(),
                Instant.now(),
                TraceId.currentOrNew(),
                orderId,
                userId,
                amount,
                status,
                transactionId);
    }
}

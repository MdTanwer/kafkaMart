package com.kafkamart.common.event;

import com.kafkamart.common.trace.TraceId;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record OrderCreated(
        @NotNull UUID eventId,
        @NotNull Instant occurredAt,
        @NotBlank String traceId,
        @NotBlank String orderId,
        @NotBlank String userId,
        @NotEmpty List<@Valid OrderItem> items,
        @NotNull BigDecimal totalAmount,
        @NotBlank String idempotencyKey,
        @NotBlank String currency) {

    public OrderCreated {
        items = List.copyOf(items);
    }

    public static OrderCreated of(
            String orderId,
            String userId,
            List<OrderItem> items,
            BigDecimal totalAmount,
            String idempotencyKey,
            String currency) {
        return new OrderCreated(
                UUID.randomUUID(),
                Instant.now(),
                TraceId.currentOrNew(),
                orderId,
                userId,
                items,
                totalAmount,
                idempotencyKey,
                currency);
    }
}

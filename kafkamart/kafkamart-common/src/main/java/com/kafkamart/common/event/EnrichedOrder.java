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

public record EnrichedOrder(
        @NotNull UUID eventId,
        @NotNull Instant occurredAt,
        @NotBlank String traceId,
        @NotBlank String orderId,
        @NotBlank String userId,
        @NotEmpty List<@Valid OrderItem> items,
        @NotNull BigDecimal totalAmount,
        @NotBlank String idempotencyKey,
        @NotBlank String currency,
        @NotBlank String userName,
        @NotBlank String userEmail) {

    public EnrichedOrder {
        items = List.copyOf(items);
    }

    public static EnrichedOrder from(OrderCreated order, String userName, String userEmail) {
        return new EnrichedOrder(
                order.eventId(),
                Instant.now(),
                TraceId.currentOrNew(),
                order.orderId(),
                order.userId(),
                order.items(),
                order.totalAmount(),
                order.idempotencyKey(),
                order.currency(),
                userName,
                userEmail);
    }
}

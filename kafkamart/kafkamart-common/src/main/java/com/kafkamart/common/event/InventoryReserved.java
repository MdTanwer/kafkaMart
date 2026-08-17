package com.kafkamart.common.event;

import com.kafkamart.common.trace.TraceId;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;
import java.util.UUID;

public record InventoryReserved(
        @NotNull UUID eventId,
        @NotNull Instant occurredAt,
        @NotBlank String traceId,
        @NotBlank String orderId,
        @NotBlank String sku,
        @Min(1) int quantity,
        @NotNull InventoryStatus status) {

    public static InventoryReserved of(
            String orderId, String sku, int quantity, InventoryStatus status) {
        return new InventoryReserved(
                UUID.randomUUID(),
                Instant.now(),
                TraceId.currentOrNew(),
                orderId,
                sku,
                quantity,
                status);
    }
}

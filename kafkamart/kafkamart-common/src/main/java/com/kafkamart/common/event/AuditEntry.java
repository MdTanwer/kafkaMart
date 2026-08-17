package com.kafkamart.common.event;

import com.kafkamart.common.trace.TraceId;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;
import java.util.UUID;

public record AuditEntry(
        @NotNull UUID eventId,
        @NotNull Instant occurredAt,
        @NotBlank String traceId,
        @NotBlank String entityId,
        @NotBlank String action,
        String payload,
        @NotNull Instant timestamp) {

    public static AuditEntry of(String entityId, String action, String payload) {
        Instant now = Instant.now();
        return new AuditEntry(
                UUID.randomUUID(), now, TraceId.currentOrNew(), entityId, action, payload, now);
    }
}

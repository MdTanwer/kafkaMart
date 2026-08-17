package com.kafkamart.common.event;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;
import java.util.UUID;

/**
 * Envelope for every KafkaMart JSON event.
 * UserProfile events use Avro instead of this record.
 */
public record EventEnvelope<T>(
        @NotNull UUID eventId,
        @NotNull Instant occurredAt,
        @NotBlank String traceId,
        @NotNull T payload
) {
    public static <T> EventEnvelope<T> of(String traceId, T payload) {
        return new EventEnvelope<>(UUID.randomUUID(), Instant.now(), traceId, payload);
    }
}

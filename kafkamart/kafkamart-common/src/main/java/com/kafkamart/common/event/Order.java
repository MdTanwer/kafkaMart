package com.kafkamart.common.event;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

public record Order(
        @NotBlank String orderId,
        @NotBlank String userId,
        @NotEmpty List<@Valid OrderLine> lines,
        @NotNull BigDecimal totalAmount,
        @NotBlank String currency,
        @NotBlank String paymentMethod,
        @NotNull OrderStatus status,
        @NotNull Instant createdAt,
        String lastReason
) {}

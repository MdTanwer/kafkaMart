package com.kafkamart.orderapi.api;

import com.kafkamart.common.event.OrderItem;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import java.math.BigDecimal;
import java.util.List;

public record CreateOrderRequest(
        @NotBlank String userId,
        @NotEmpty List<@Valid OrderItem> items,
        BigDecimal totalAmount,
        String currency,
        String idempotencyKey) {}

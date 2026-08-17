package com.kafkamart.common.event;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;

public record OrderLine(
        @NotBlank String sku,
        @Min(1) int quantity,
        @NotNull BigDecimal unitPrice
) {}

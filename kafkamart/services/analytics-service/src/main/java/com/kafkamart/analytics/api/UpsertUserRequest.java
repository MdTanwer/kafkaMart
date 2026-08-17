package com.kafkamart.analytics.api;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;

public record UpsertUserRequest(
        @NotBlank String userId, @NotBlank String name, @Email @NotBlank String email) {}

package com.kafkamart.userprofile.api;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;

public record UpsertUserRequest(
        @NotBlank String userId, @NotBlank String name, @NotBlank @Email String email) {}

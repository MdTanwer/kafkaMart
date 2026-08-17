package com.kafkamart.orderapi.domain;

public enum OrderStatus {
    PENDING,
    RESERVED,
    PAID,
    REJECTED,
    CANCELLED,
    SHIPPED
}

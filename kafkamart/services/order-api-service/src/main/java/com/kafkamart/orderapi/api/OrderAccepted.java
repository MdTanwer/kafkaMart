package com.kafkamart.orderapi.api;

import java.util.List;

public record OrderAccepted(String orderId) {
    public record LoadAccepted(int count, List<String> orderIds) {}
}

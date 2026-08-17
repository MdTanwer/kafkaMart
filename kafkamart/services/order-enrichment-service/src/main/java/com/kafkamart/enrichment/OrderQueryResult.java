package com.kafkamart.enrichment;

import com.kafkamart.common.event.EnrichedOrder;

public record OrderQueryResult(
        EnrichedOrder order, String host, int port, String reason, boolean found) {

    static OrderQueryResult hit(EnrichedOrder order, String host, int port) {
        return new OrderQueryResult(order, host, port, null, true);
    }

    static OrderQueryResult miss(String orderId, String host, int port, String reason) {
        return new OrderQueryResult(null, host, port, reason, false);
    }

    public OrderQueryMiss missBody(String orderId) {
        return new OrderQueryMiss(orderId, host, port, reason);
    }
}

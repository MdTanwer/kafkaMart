package com.kafkamart.enrichment;

public record OrderQueryMiss(String orderId, String host, int port, String reason) {}

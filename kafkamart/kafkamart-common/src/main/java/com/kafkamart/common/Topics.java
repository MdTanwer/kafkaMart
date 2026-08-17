package com.kafkamart.common;

/** KafkaMart topic names. Topics are created only via scripts/create-topics.sh. */
public final class Topics {
    public static final String ORDERS = "orders";
    public static final String USERS = "users";
    public static final String PAYMENTS = "payments";
    public static final String INVENTORY_EVENTS = "inventory-events";
    public static final String FRAUD_ALERTS = "fraud-alerts";
    public static final String ORDERS_ENRICHED = "orders-enriched";
    public static final String SHIPMENTS = "shipments";
    public static final String ORDERS_DLQ = "orders-dlq";
    public static final String ORDERS_RETRY_5S = "orders-retry-5s";
    public static final String ORDERS_RETRY_1M = "orders-retry-1m";
    public static final String AUDIT_LOG = "audit-log";
    public static final String USERS_CDC = "users-cdc";

    private Topics() {}
}

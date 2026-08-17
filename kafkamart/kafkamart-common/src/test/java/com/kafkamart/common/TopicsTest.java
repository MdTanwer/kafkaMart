package com.kafkamart.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class TopicsTest {
    @Test
    void exposesAllTwelveTopics() throws IllegalAccessException {
        Set<String> expected =
                Set.of(
                        "orders",
                        "users",
                        "payments",
                        "inventory-events",
                        "fraud-alerts",
                        "orders-enriched",
                        "shipments",
                        "orders-dlq",
                        "orders-retry-5s",
                        "orders-retry-1m",
                        "audit-log",
                        "users-cdc");
        Set<String> actual = new HashSet<>();
        for (Field field : Topics.class.getFields()) {
            if (Modifier.isStatic(field.getModifiers()) && field.getType().equals(String.class)) {
                actual.add((String) field.get(null));
            }
        }
        assertEquals(12, actual.size());
        assertTrue(actual.containsAll(expected));
        assertEquals("orders", Topics.ORDERS);
        assertEquals("users-cdc", Topics.USERS_CDC);
    }
}

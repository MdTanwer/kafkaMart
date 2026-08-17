package com.kafkamart.common;

import org.slf4j.MDC;

import java.util.UUID;

public final class TraceIds {
    public static final String MDC_KEY = "traceId";

    private TraceIds() {}

    public static String currentOrNew() {
        String existing = MDC.get(MDC_KEY);
        if (existing != null && !existing.isBlank()) {
            return existing;
        }
        String created = UUID.randomUUID().toString();
        MDC.put(MDC_KEY, created);
        return created;
    }

    public static void set(String traceId) {
        if (traceId != null && !traceId.isBlank()) {
            MDC.put(MDC_KEY, traceId);
        }
    }

    public static void clear() {
        MDC.remove(MDC_KEY);
    }
}

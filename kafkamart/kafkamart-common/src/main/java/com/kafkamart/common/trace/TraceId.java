package com.kafkamart.common.trace;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.MDC;

/**
 * Correlation id stored in a ThreadLocal (always) and mirrored into SLF4J MDC when a binding is
 * present. Kafka header name is {@code traceId}.
 */
public final class TraceId {
    public static final String HEADER = "traceId";
    public static final String MDC_KEY = "traceId";

    private static final ThreadLocal<String> CURRENT = new ThreadLocal<>();

    private TraceId() {}

    public static String currentOrNew() {
        String existing = current();
        if (existing != null && !existing.isBlank()) {
            return existing;
        }
        String created = UUID.randomUUID().toString();
        set(created);
        return created;
    }

    public static String current() {
        String local = CURRENT.get();
        if (local != null && !local.isBlank()) {
            return local;
        }
        return MDC.get(MDC_KEY);
    }

    public static void set(String traceId) {
        if (traceId == null || traceId.isBlank()) {
            return;
        }
        CURRENT.set(traceId);
        MDC.put(MDC_KEY, traceId);
    }

    public static void clear() {
        CURRENT.remove();
        MDC.remove(MDC_KEY);
    }

    public static void applyTo(Headers headers) {
        String traceId = currentOrNew();
        headers.remove(HEADER);
        headers.add(HEADER, traceId.getBytes(StandardCharsets.UTF_8));
    }

    public static void restoreFrom(Headers headers) {
        Header header = headers.lastHeader(HEADER);
        if (header != null && header.value() != null && header.value().length > 0) {
            set(new String(header.value(), StandardCharsets.UTF_8));
        }
    }
}

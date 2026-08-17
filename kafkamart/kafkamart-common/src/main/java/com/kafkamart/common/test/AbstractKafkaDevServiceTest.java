package com.kafkamart.common.test;

import com.kafkamart.common.trace.TraceId;
import com.kafkamart.common.trace.TraceIdFilter;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Base class for {@code @QuarkusTest} classes that rely on Kafka Dev Services. Subclasses must
 * still be annotated with {@code @QuarkusTest}; this module is a plain library.
 */
public abstract class AbstractKafkaDevServiceTest {
    public static final String TRACE_ID_HEADER = TraceIdFilter.HEADER;
    public static final String DEFAULT_BOOTSTRAP = "localhost:9092,localhost:9094,localhost:9096";

    protected String bootstrapServers() {
        String configured =
                ConfigProvider.getConfig()
                        .getOptionalValue("kafka.bootstrap.servers", String.class)
                        .or(
                                () ->
                                        ConfigProvider.getConfig()
                                                .getOptionalValue(
                                                        "mp.messaging.connector.smallrye-kafka.bootstrap.servers",
                                                        String.class))
                        .orElse(DEFAULT_BOOTSTRAP);
        return stripListenerScheme(configured);
    }

    /**
     * Redpanda Dev Services advertises {@code OUTSIDE://host:port}. The Apache Kafka client wants
     * {@code host:port}.
     */
    protected static String stripListenerScheme(String bootstrap) {
        if (bootstrap == null || bootstrap.isBlank()) {
            return DEFAULT_BOOTSTRAP;
        }
        return bootstrap.replaceAll("(?i)[A-Za-z][A-Za-z0-9+.-]*://", "");
    }

    protected String bindTraceId() {
        return TraceId.currentOrNew();
    }

    protected void clearTraceId() {
        TraceId.clear();
    }
}

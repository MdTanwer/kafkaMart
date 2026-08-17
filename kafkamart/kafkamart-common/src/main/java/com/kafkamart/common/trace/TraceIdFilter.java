package com.kafkamart.common.trace;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.ext.Provider;
import java.io.IOException;

/**
 * JAX-RS filter that binds {@code traceId} into MDC. Pair with {@link TraceIdProducerInterceptor} /
 * {@link TraceIdConsumerInterceptor} so the same id is copied onto Kafka records (header {@code
 * traceId}) and restored on consume.
 */
@Provider
@Priority(1)
@ApplicationScoped
public class TraceIdFilter implements ContainerRequestFilter, ContainerResponseFilter {
    public static final String HEADER = TraceId.HEADER;
    public static final String MDC_KEY = TraceId.MDC_KEY;
    public static final String KAFKA_PRODUCER_INTERCEPTOR =
            TraceIdProducerInterceptor.class.getName();
    public static final String KAFKA_CONSUMER_INTERCEPTOR =
            TraceIdConsumerInterceptor.class.getName();

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        String traceId = requestContext.getHeaderString(HEADER);
        if (traceId == null || traceId.isBlank()) {
            traceId = requestContext.getHeaderString("X-Trace-Id");
        }
        if (traceId == null || traceId.isBlank()) {
            traceId = java.util.UUID.randomUUID().toString();
        }
        TraceId.set(traceId);
        requestContext.setProperty(MDC_KEY, traceId);
    }

    @Override
    public void filter(
            ContainerRequestContext requestContext, ContainerResponseContext responseContext)
            throws IOException {
        String traceId = TraceId.current();
        if (traceId == null || traceId.isBlank()) {
            Object property = requestContext.getProperty(MDC_KEY);
            traceId = property == null ? null : property.toString();
        }
        if (traceId != null && !traceId.isBlank()) {
            responseContext.getHeaders().putSingle(HEADER, traceId);
        }
        TraceId.clear();
    }
}

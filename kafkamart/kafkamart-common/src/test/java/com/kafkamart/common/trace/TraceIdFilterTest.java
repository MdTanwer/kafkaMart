package com.kafkamart.common.trace;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class TraceIdFilterTest {
    private final TraceIdFilter filter = new TraceIdFilter();

    @AfterEach
    void clear() {
        TraceId.clear();
    }

    @Test
    void jaxRsFilterUsesIncomingHeaderAndEchoesOnResponse() throws Exception {
        ContainerRequestContext request = mock(ContainerRequestContext.class);
        when(request.getHeaderString(TraceId.HEADER)).thenReturn("trace-abc");

        filter.filter(request);
        assertEquals("trace-abc", TraceId.current());
        verify(request).setProperty(TraceId.MDC_KEY, "trace-abc");

        ContainerResponseContext response = mock(ContainerResponseContext.class);
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
        when(response.getHeaders()).thenReturn(headers);

        filter.filter(request, response);
        assertEquals("trace-abc", headers.getFirst(TraceId.HEADER));
    }

    @Test
    void producerInterceptorWritesMdcToKafkaHeader() {
        TraceId.set("trace-xyz");
        TraceIdProducerInterceptor interceptor = new TraceIdProducerInterceptor();
        ProducerRecord<Object, Object> record = new ProducerRecord<>("orders", "key", "value");
        interceptor.onSend(record);
        assertEquals(
                "trace-xyz",
                new String(
                        record.headers().lastHeader(TraceId.HEADER).value(),
                        StandardCharsets.UTF_8));
    }

    @Test
    void generatesTraceIdWhenRequestHasNone() throws Exception {
        ContainerRequestContext request = mock(ContainerRequestContext.class);
        when(request.getHeaderString(TraceId.HEADER)).thenReturn(null);
        when(request.getHeaderString("X-Trace-Id")).thenReturn(null);
        filter.filter(request);
        assertNotNull(TraceId.current());
    }
}

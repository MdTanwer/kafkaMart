package com.kafkamart.common.trace;

import java.util.Map;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/** Copies MDC {@code traceId} onto the Kafka record header of the same name. */
public class TraceIdProducerInterceptor implements ProducerInterceptor<Object, Object> {
    @Override
    public ProducerRecord<Object, Object> onSend(ProducerRecord<Object, Object> record) {
        if (record.headers().lastHeader(TraceId.HEADER) == null) {
            TraceId.applyTo(record.headers());
        }
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        // no-op
    }

    @Override
    public void close() {
        // no-op
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // no-op
    }
}

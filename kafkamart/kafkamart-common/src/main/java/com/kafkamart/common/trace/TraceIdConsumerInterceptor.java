package com.kafkamart.common.trace;

import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/** Restores Kafka header {@code traceId} into MDC when records are consumed. */
public class TraceIdConsumerInterceptor implements ConsumerInterceptor<Object, Object> {
    @Override
    public ConsumerRecords<Object, Object> onConsume(ConsumerRecords<Object, Object> records) {
        for (var record : records) {
            TraceId.restoreFrom(record.headers());
        }
        return records;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        TraceId.clear();
    }

    @Override
    public void close() {
        TraceId.clear();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // no-op
    }
}

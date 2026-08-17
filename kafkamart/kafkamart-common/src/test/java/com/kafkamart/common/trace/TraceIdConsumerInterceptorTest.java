package com.kafkamart.common.trace;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class TraceIdConsumerInterceptorTest {
    @AfterEach
    void clear() {
        TraceId.clear();
    }

    @Test
    void restoresTraceIdFromRecordHeader() {
        ConsumerRecord<Object, Object> record = new ConsumerRecord<>("orders", 0, 0L, "k", "v");
        record.headers().add(TraceId.HEADER, "from-kafka".getBytes(StandardCharsets.UTF_8));
        TopicPartition tp = new TopicPartition("orders", 0);
        ConsumerRecords<Object, Object> records =
                new ConsumerRecords<>(Map.of(tp, List.of(record)));
        new TraceIdConsumerInterceptor().onConsume(records);
        assertEquals("from-kafka", TraceId.current());
    }
}

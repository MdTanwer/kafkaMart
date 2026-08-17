package com.kafkamart.fraud;

import com.kafkamart.common.event.OrderCreated;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Event-time from {@link OrderCreated#occurredAt()}, not the broker timestamp. Required for the
 * late-event demo: a record produced "now" with an old {@code occurredAt} is late relative to
 * stream time.
 */
public final class OrderCreatedTimestampExtractor implements TimestampExtractor {
    private static final Logger LOG = LoggerFactory.getLogger(OrderCreatedTimestampExtractor.class);
    private static final long LAG_LOG_THRESHOLD_MS = 60_000L;

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        Object value = record.value();
        if (value instanceof OrderCreated order && order.occurredAt() != null) {
            long eventTime = order.occurredAt().toEpochMilli();
            long lagMs = record.timestamp() - eventTime;
            if (lagMs >= LAG_LOG_THRESHOLD_MS) {
                LOG.info(
                        "LATE_CANDIDATE orderId={} userId={} occurredAt={} kafkaTs={} lagMs={}",
                        order.orderId(),
                        order.userId(),
                        order.occurredAt(),
                        record.timestamp(),
                        lagMs);
            }
            return eventTime;
        }
        long fallback = record.timestamp() >= 0 ? record.timestamp() : partitionTime;
        LOG.warn(
                "timestamp extractor fallback topic={} partition={} offset={} ts={}",
                record.topic(),
                record.partition(),
                record.offset(),
                fallback);
        return fallback;
    }
}

package com.kafkamart.notification;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.kafka.KafkaConsumerRebalanceListener;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RangeAssignor (eager): any group membership change revokes <em>all</em> partitions from every
 * remaining member, then assigns a new complete set. Contrast with {@link EmailRebalanceListener}.
 */
@ApplicationScoped
@Identifier("payments-sms") public class SmsRebalanceListener implements KafkaConsumerRebalanceListener {
    private static final Logger LOG = LoggerFactory.getLogger(SmsRebalanceListener.class);

    private final CopyOnWriteArrayList<String> events = new CopyOnWriteArrayList<>();

    @Override
    public void onPartitionsAssigned(
            Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        String line =
                "REBALANCE assigned group=notification-sms assignor=eager-range partitions="
                        + partitions
                        + " — EAGER: full new assignment after stop-the-world revoke";
        events.add(line);
        LOG.info(line);
    }

    @Override
    public void onPartitionsRevoked(
            Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        String line =
                "REBALANCE revoked group=notification-sms assignor=eager-range partitions="
                        + partitions
                        + " — EAGER: typically ALL currently owned partitions (stop-the-world)";
        events.add(line);
        LOG.info(line);
    }

    @Override
    public void onPartitionsLost(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        String line =
                "REBALANCE lost group=notification-sms assignor=eager-range partitions="
                        + partitions
                        + " — member kicked (max.poll.interval.ms or session timeout)";
        events.add(line);
        LOG.warn(line);
    }

    public List<String> events() {
        return List.copyOf(events);
    }
}

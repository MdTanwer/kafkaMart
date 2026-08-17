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
 * CooperativeStickyAssignor: on a member join/leave, this member typically keeps partitions it
 * still owns. {@code onPartitionsRevoked} lists only partitions that are actually moving
 * (incremental), not the full assignment.
 */
@ApplicationScoped
@Identifier("payments-email") public class EmailRebalanceListener implements KafkaConsumerRebalanceListener {
    private static final Logger LOG = LoggerFactory.getLogger(EmailRebalanceListener.class);

    private final CopyOnWriteArrayList<String> events = new CopyOnWriteArrayList<>();

    @Override
    public void onPartitionsAssigned(
            Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        String line =
                "REBALANCE assigned group=notification-email assignor=cooperative-sticky partitions="
                        + partitions
                        + " — incremental: only newly owned partitions appear here";
        events.add(line);
        LOG.info(line);
    }

    @Override
    public void onPartitionsRevoked(
            Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        String line =
                "REBALANCE revoked group=notification-email assignor=cooperative-sticky partitions="
                        + partitions
                        + " — COOPERATIVE: empty or a subset (not a stop-the-world revoke of every"
                        + " partition)";
        events.add(line);
        LOG.info(line);
    }

    @Override
    public void onPartitionsLost(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        String line =
                "REBALANCE lost group=notification-email assignor=cooperative-sticky partitions="
                        + partitions;
        events.add(line);
        LOG.warn(line);
    }

    public List<String> events() {
        return List.copyOf(events);
    }
}

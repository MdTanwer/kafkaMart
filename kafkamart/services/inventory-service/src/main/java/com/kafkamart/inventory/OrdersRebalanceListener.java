package com.kafkamart.inventory;

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
 * Bound to channel {@code orders-in}. Logs every assignment/revocation so a rolling restart makes
 * the rebalance visible, and flushes commits before partitions are taken away.
 */
@ApplicationScoped
@Identifier("orders-in") public class OrdersRebalanceListener implements KafkaConsumerRebalanceListener {
    private static final Logger LOG = LoggerFactory.getLogger(OrdersRebalanceListener.class);

    private final CopyOnWriteArrayList<String> events = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<TopicPartition> assigned = new CopyOnWriteArrayList<>();

    @Override
    public void onPartitionsAssigned(
            Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        assigned.clear();
        assigned.addAll(partitions);
        String line =
                "REBALANCE assigned group=inventory-service partitions="
                        + partitions
                        + " — this member now owns these orders partitions";
        events.add(line);
        LOG.info(line);
    }

    @Override
    public void onPartitionsRevoked(
            Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        String line =
                "REBALANCE revoked group=inventory-service partitions="
                        + partitions
                        + " — flushing commits before giving up partitions";
        events.add(line);
        LOG.info(line);
        if (consumer != null) {
            consumer.commitSync();
            LOG.info("REBALANCE flushed commits for revoked partitions={}", partitions);
        }
        assigned.removeAll(partitions);
    }

    public List<String> events() {
        return List.copyOf(events);
    }

    public List<TopicPartition> assigned() {
        return List.copyOf(assigned);
    }
}

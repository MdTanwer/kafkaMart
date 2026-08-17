package com.kafkamart.inventory;

import com.kafkamart.common.event.InventoryReserved;
import com.kafkamart.common.event.OrderCreated;
import com.kafkamart.common.trace.TraceId;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.OnOverflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class InventoryConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(InventoryConsumer.class);

    @Inject InventoryProcessor processor;
    @Inject ServiceMetrics metrics;

    @Inject
    @Channel("inventory-out")
    @OnOverflow(value = OnOverflow.Strategy.BUFFER, bufferSize = 4096)
    MutinyEmitter<InventoryReserved> inventoryOut;

    @Incoming("orders-in")
    @Blocking
    public CompletionStage<Void> onOrder(Message<OrderCreated> message) {
        IncomingKafkaRecordMetadata<?, ?> metadata =
                message.getMetadata(IncomingKafkaRecordMetadata.class).orElse(null);
        if (metadata == null) {
            LOG.error("orders-in message missing IncomingKafkaRecordMetadata — nack");
            return message.nack(new IllegalStateException("missing Kafka metadata"));
        }
        TraceId.restoreFrom(metadata.getHeaders());
        OrderCreated order = message.getPayload();
        if (order.traceId() != null) {
            TraceId.set(order.traceId());
        }
        int partition = metadata.getPartition();
        long offset = metadata.getOffset();
        try {
            InventoryProcessor.ProcessResult result = processor.process(partition, offset, order);
            if (!result.skipped()) {
                for (InventoryReserved event : result.events()) {
                    emit(event);
                }
                metrics.consumed();
            } else {
                LOG.info(
                        "DEDUP skip+ack partition={} offset={} orderId={}",
                        partition,
                        offset,
                        order.orderId());
            }
            return message.ack();
        } catch (RuntimeException failure) {
            LOG.error(
                    "inventory processing failed partition={} offset={} orderId={}",
                    partition,
                    offset,
                    order.orderId(),
                    failure);
            metrics.failed();
            return message.nack(failure);
        } finally {
            TraceId.clear();
        }
    }

    private void emit(InventoryReserved event) {
        var headers = new RecordHeaders();
        headers.add(TraceId.HEADER, event.traceId().getBytes(StandardCharsets.UTF_8));
        OutgoingKafkaRecordMetadata<String> metadata =
                OutgoingKafkaRecordMetadata.<String>builder()
                        .withKey(event.orderId())
                        .withHeaders(headers)
                        .build();
        inventoryOut
                .sendMessage(Message.of(event).addMetadata(metadata))
                .await()
                .atMost(Duration.ofSeconds(15));
        metrics.produced();
    }
}

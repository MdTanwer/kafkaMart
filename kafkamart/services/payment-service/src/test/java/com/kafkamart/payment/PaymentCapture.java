package com.kafkamart.payment;

import com.kafkamart.common.event.PaymentCompleted;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

/**
 * Test sink on {@code payments} with {@code isolation.level=read_committed} so aborted
 * transactional records are never counted.
 */
@ApplicationScoped
public class PaymentCapture {
    public record Captured(String key, PaymentCompleted event, int partition, long offset) {}

    private final CopyOnWriteArrayList<Captured> captured = new CopyOnWriteArrayList<>();

    @Incoming("payments-verify")
    public CompletionStage<Void> onPayment(Message<PaymentCompleted> message) {
        IncomingKafkaRecordMetadata<?, ?> metadata =
                message.getMetadata(IncomingKafkaRecordMetadata.class).orElse(null);
        String key =
                metadata == null || metadata.getKey() == null ? null : metadata.getKey().toString();
        int partition = metadata == null ? -1 : metadata.getPartition();
        long offset = metadata == null ? -1L : metadata.getOffset();
        captured.add(new Captured(key, message.getPayload(), partition, offset));
        return message.ack();
    }

    public List<Captured> snapshot() {
        return List.copyOf(captured);
    }

    public long countByOrderId(String orderId) {
        return snapshot().stream().filter(c -> orderId.equals(c.event().orderId())).count();
    }
}

package com.kafkamart.inventory;

import com.kafkamart.common.event.InventoryReserved;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

@ApplicationScoped
public class InventoryEventCapture {
    public record Captured(String key, InventoryReserved event, int partition) {}

    private final CopyOnWriteArrayList<Captured> captured = new CopyOnWriteArrayList<>();

    @Incoming("inventory-verify")
    public CompletionStage<Void> onEvent(Message<InventoryReserved> message) {
        IncomingKafkaRecordMetadata<?, ?> metadata =
                message.getMetadata(IncomingKafkaRecordMetadata.class).orElse(null);
        String key =
                metadata == null || metadata.getKey() == null ? null : metadata.getKey().toString();
        int partition = metadata == null ? -1 : metadata.getPartition();
        captured.add(new Captured(key, message.getPayload(), partition));
        return message.ack();
    }

    public List<Captured> snapshot() {
        return List.copyOf(captured);
    }
}

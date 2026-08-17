package com.kafkamart.userprofile;

import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

/** Test sink that reads raw {@code users} values to assert Confluent wire format (magic byte). */
@ApplicationScoped
public class UserWireCapture {
    public record Captured(String key, byte[] value) {}

    private final CopyOnWriteArrayList<Captured> captured = new CopyOnWriteArrayList<>();

    @Incoming("users-wire")
    public CompletionStage<Void> onBytes(Message<byte[]> message) {
        IncomingKafkaRecordMetadata<?, ?> metadata =
                message.getMetadata(IncomingKafkaRecordMetadata.class).orElse(null);
        String key =
                metadata == null || metadata.getKey() == null ? null : metadata.getKey().toString();
        captured.add(new Captured(key, message.getPayload()));
        return message.ack();
    }

    public List<Captured> snapshot() {
        return List.copyOf(captured);
    }
}

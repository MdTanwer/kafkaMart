package com.kafkamart.orderapi;

import com.kafkamart.common.event.OrderCreated;
import com.kafkamart.common.trace.TraceId;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.kafka.common.header.Header;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

/**
 * Test-only sink that shares the application's Kafka client / Dev Services broker so we can assert
 * key, payload, partition, and the {@code traceId} header without a second bootstrap string.
 */
@ApplicationScoped
public class OrderCapture {
    public record Captured(
            String key, OrderCreated event, String traceId, int partition, long offset) {}

    private final CopyOnWriteArrayList<Captured> captured = new CopyOnWriteArrayList<>();

    @Incoming("orders-verify")
    public CompletionStage<Void> onMessage(Message<OrderCreated> message) {
        IncomingKafkaRecordMetadata<?, ?> metadata =
                message.getMetadata(IncomingKafkaRecordMetadata.class).orElseThrow();
        Header header = metadata.getHeaders().lastHeader(TraceId.HEADER);
        String traceId =
                header == null || header.value() == null
                        ? null
                        : new String(header.value(), StandardCharsets.UTF_8);
        Object key = metadata.getKey();
        captured.add(
                new Captured(
                        key == null ? null : key.toString(),
                        message.getPayload(),
                        traceId,
                        metadata.getPartition(),
                        metadata.getOffset()));
        return message.ack();
    }

    public List<Captured> snapshot() {
        return List.copyOf(captured);
    }
}

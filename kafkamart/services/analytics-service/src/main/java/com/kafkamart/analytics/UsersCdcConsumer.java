package com.kafkamart.analytics;

import com.kafkamart.common.trace.TraceId;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.vertx.core.json.JsonObject;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class UsersCdcConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(UsersCdcConsumer.class);

    @Inject CdcBuffer buffer;
    @Inject ServiceMetrics metrics;

    @Incoming("users-cdc-in")
    @Blocking
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public CompletionStage<Void> onCdc(Message<String> message) {
        IncomingKafkaRecordMetadata<?, ?> metadata =
                message.getMetadata(IncomingKafkaRecordMetadata.class).orElse(null);
        if (metadata != null) {
            TraceId.restoreFrom(metadata.getHeaders());
        }
        try {
            Map<String, Object> row = new LinkedHashMap<>();
            if (metadata != null) {
                row.put("key", metadata.getKey());
                row.put("offset", metadata.getOffset());
                row.put("partition", metadata.getPartition());
            }
            String raw = message.getPayload();
            if (raw != null && !raw.isBlank()) {
                try {
                    row.put("value", new JsonObject(raw).getMap());
                } catch (RuntimeException notJson) {
                    row.put("value", raw);
                }
            }
            buffer.add(row);
            metrics.consumed();
            LOG.info("CDC users-cdc key={} payload={}", row.get("key"), raw);
            return message.ack();
        } catch (RuntimeException failure) {
            LOG.error("CDC consume failed", failure);
            return message.nack(failure);
        } finally {
            TraceId.clear();
        }
    }
}

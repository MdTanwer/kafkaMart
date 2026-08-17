package com.kafkamart.userprofile;

import com.kafkamart.avro.UserProfile;
import com.kafkamart.common.trace.TraceId;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.concurrent.CompletionStage;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rebuilds the user table from compacted {@code users}. Redelivery is a put of the same key
 * (idempotent last-write-wins).
 */
@ApplicationScoped
public class UserProfileConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(UserProfileConsumer.class);

    @Inject UserProfileCache cache;
    @Inject ServiceMetrics metrics;

    @Incoming("users-in")
    @Blocking
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public CompletionStage<Void> onProfile(Message<UserProfile> message) {
        IncomingKafkaRecordMetadata<?, ?> metadata =
                message.getMetadata(IncomingKafkaRecordMetadata.class).orElse(null);
        if (metadata != null) {
            TraceId.restoreFrom(metadata.getHeaders());
        }
        UserProfile profile = message.getPayload();
        try {
            cache.put(profile);
            metrics.consumed();
            LOG.info(
                    "cache upsert user_id={} name={} (compacted table)",
                    profile.getUserId(),
                    profile.getName());
            return message.ack();
        } catch (RuntimeException failure) {
            LOG.error("cache upsert failed user_id={}", profile.getUserId(), failure);
            return message.nack(failure);
        } finally {
            TraceId.clear();
        }
    }
}

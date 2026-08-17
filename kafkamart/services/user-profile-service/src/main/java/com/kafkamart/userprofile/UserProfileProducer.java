package com.kafkamart.userprofile;

import com.kafkamart.avro.UserProfile;
import com.kafkamart.common.trace.TraceId;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.OnOverflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class UserProfileProducer {
    private static final Logger LOG = LoggerFactory.getLogger(UserProfileProducer.class);

    @Inject ServiceMetrics metrics;

    @Inject
    @Channel("users-out")
    @OnOverflow(value = OnOverflow.Strategy.BUFFER, bufferSize = 4096)
    MutinyEmitter<UserProfile> usersOut;

    public UserProfile upsert(String userId, String name, String email) {
        UserProfile profile =
                UserProfile.newBuilder().setUserId(userId).setName(name).setEmail(email).build();
        var headers = new RecordHeaders();
        headers.add(TraceId.HEADER, TraceId.currentOrNew().getBytes(StandardCharsets.UTF_8));
        OutgoingKafkaRecordMetadata<String> metadata =
                OutgoingKafkaRecordMetadata.<String>builder()
                        .withKey(userId)
                        .withHeaders(headers)
                        .build();
        usersOut.sendMessage(Message.of(profile).addMetadata(metadata))
                .await()
                .atMost(Duration.ofSeconds(20));
        metrics.produced();
        LOG.info("emitted UserProfile user_id={} key=user_id", userId);
        return profile;
    }
}

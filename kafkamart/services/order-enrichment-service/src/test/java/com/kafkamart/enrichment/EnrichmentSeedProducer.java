package com.kafkamart.enrichment;

import com.kafkamart.avro.UserProfile;
import com.kafkamart.common.event.OrderCreated;
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

@ApplicationScoped
public class EnrichmentSeedProducer {
    @Inject
    @Channel("orders-seed")
    MutinyEmitter<OrderCreated> orders;

    @Inject
    @Channel("users-seed")
    MutinyEmitter<UserProfile> users;

    public void sendOrder(OrderCreated order) {
        send(orders, order.userId(), order.traceId(), order);
    }

    public void sendUser(String userId, String name, String email) {
        UserProfile profile =
                UserProfile.newBuilder().setUserId(userId).setName(name).setEmail(email).build();
        send(users, userId, TraceId.currentOrNew(), profile);
    }

    private static <T> void send(MutinyEmitter<T> emitter, String key, String traceId, T payload) {
        var headers = new RecordHeaders();
        headers.add(TraceId.HEADER, traceId.getBytes(StandardCharsets.UTF_8));
        OutgoingKafkaRecordMetadata<String> metadata =
                OutgoingKafkaRecordMetadata.<String>builder()
                        .withKey(key)
                        .withHeaders(headers)
                        .build();
        emitter.sendMessage(Message.of(payload).addMetadata(metadata))
                .await()
                .atMost(Duration.ofSeconds(20));
    }
}

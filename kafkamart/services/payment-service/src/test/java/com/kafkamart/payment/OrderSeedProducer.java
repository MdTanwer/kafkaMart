package com.kafkamart.payment;

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
public class OrderSeedProducer {
    @Inject
    @Channel("orders-seed")
    MutinyEmitter<OrderCreated> orders;

    public void send(OrderCreated order) {
        var headers = new RecordHeaders();
        headers.add(TraceId.HEADER, order.traceId().getBytes(StandardCharsets.UTF_8));
        OutgoingKafkaRecordMetadata<String> metadata =
                OutgoingKafkaRecordMetadata.<String>builder()
                        .withKey(order.userId())
                        .withHeaders(headers)
                        .build();
        orders.sendMessage(Message.of(order).addMetadata(metadata))
                .await()
                .atMost(Duration.ofSeconds(15));
    }
}

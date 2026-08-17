package com.kafkamart.orderapi;

import com.kafkamart.common.event.OrderCreated;
import com.kafkamart.common.trace.TraceId;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.OnOverflow;

@ApplicationScoped
public class OrderProducer {
    @Inject ServiceMetrics metrics;

    @Inject
    @Channel("orders-out")
    @OnOverflow(value = OnOverflow.Strategy.BUFFER, bufferSize = 4096)
    MutinyEmitter<OrderCreated> ordersOut;

    public CompletionStage<String> send(OrderCreated event) {
        var headers = new RecordHeaders();
        headers.add(TraceId.HEADER, event.traceId().getBytes(StandardCharsets.UTF_8));
        OutgoingKafkaRecordMetadata<String> metadata =
                OutgoingKafkaRecordMetadata.<String>builder()
                        .withKey(event.userId())
                        .withHeaders(headers)
                        .build();
        Message<OrderCreated> record =
                Message.of(event)
                        .addMetadata(metadata)
                        .withAck(
                                () -> {
                                    metrics.orderProduced("success");
                                    return CompletableFuture.completedFuture(null);
                                })
                        .withNack(
                                throwable -> {
                                    metrics.orderProduced("failure");
                                    return CompletableFuture.completedFuture(null);
                                });
        return ordersOut
                .sendMessage(record)
                .subscribeAsCompletionStage()
                .thenApply(unused -> event.orderId());
    }
}

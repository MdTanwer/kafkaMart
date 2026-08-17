package com.kafkamart.notification;

import com.kafkamart.common.event.PaymentCompleted;
import com.kafkamart.common.event.ShipmentCreated;
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
public class NotificationSeedProducer {
    @Inject
    @Channel("payments-seed")
    MutinyEmitter<PaymentCompleted> payments;

    @Inject
    @Channel("shipments-seed")
    MutinyEmitter<ShipmentCreated> shipments;

    public void sendPayment(PaymentCompleted payment) {
        send(payments, payment.orderId(), payment.traceId(), payment);
    }

    public void sendShipment(ShipmentCreated shipment) {
        send(shipments, shipment.orderId(), shipment.traceId(), shipment);
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
                .atMost(Duration.ofSeconds(15));
    }
}

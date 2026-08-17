package com.kafkamart.notification;

import com.kafkamart.common.event.ShipmentCreated;
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

@ApplicationScoped
public class ShipmentConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(ShipmentConsumer.class);
    static final String CHANNEL = "shipments-in";

    @Inject NotificationIdempotency idempotency;
    @Inject NotificationRecorder recorder;
    @Inject ServiceMetrics metrics;

    @Incoming("shipments-in")
    @Blocking
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public CompletionStage<Void> onShipment(Message<ShipmentCreated> message) {
        IncomingKafkaRecordMetadata<?, ?> metadata =
                message.getMetadata(IncomingKafkaRecordMetadata.class).orElse(null);
        if (metadata != null) {
            TraceId.restoreFrom(metadata.getHeaders());
        }
        ShipmentCreated shipment = message.getPayload();
        try {
            if (!idempotency.firstTime(CHANNEL, shipment.eventId())) {
                metrics.duplicate();
                LOG.info(
                        "DEDUP skip+ack tracking eventId={} orderId={}",
                        shipment.eventId(),
                        shipment.orderId());
                return message.ack();
            }
            LOG.info(
                    "tracking link sent shipmentId={} orderId={} userId={} group=notification-email",
                    shipment.shipmentId(),
                    shipment.orderId(),
                    shipment.userId());
            recorder.tracking(shipment.eventId());
            metrics.tracking();
            return message.ack();
        } catch (RuntimeException failure) {
            LOG.error("tracking notify failed orderId={}", shipment.orderId(), failure);
            return message.nack(failure);
        } finally {
            TraceId.clear();
        }
    }
}

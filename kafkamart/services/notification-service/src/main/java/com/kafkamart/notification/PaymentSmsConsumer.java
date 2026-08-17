package com.kafkamart.notification;

import com.kafkamart.common.event.PaymentCompleted;
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
public class PaymentSmsConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(PaymentSmsConsumer.class);
    static final String CHANNEL = "payments-sms";

    @Inject NotificationIdempotency idempotency;
    @Inject NotificationRecorder recorder;
    @Inject ServiceMetrics metrics;
    @Inject SlowChaos chaos;

    @Incoming("payments-sms")
    @Blocking
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public CompletionStage<Void> onPayment(Message<PaymentCompleted> message) {
        IncomingKafkaRecordMetadata<?, ?> metadata =
                message.getMetadata(IncomingKafkaRecordMetadata.class).orElse(null);
        if (metadata != null) {
            TraceId.restoreFrom(metadata.getHeaders());
        }
        PaymentCompleted payment = message.getPayload();
        try {
            if (!idempotency.firstTime(CHANNEL, payment.eventId())) {
                metrics.duplicate();
                LOG.info(
                        "DEDUP skip+ack sms eventId={} orderId={}",
                        payment.eventId(),
                        payment.orderId());
                return message.ack();
            }
            chaos.maybeSlow();
            LOG.info(
                    "SMS sent orderId={} userId={} status={} group=notification-sms",
                    payment.orderId(),
                    payment.userId(),
                    payment.status());
            recorder.sms(payment.eventId());
            metrics.sms();
            return message.ack();
        } catch (RuntimeException failure) {
            LOG.error("sms notify failed orderId={}", payment.orderId(), failure);
            return message.nack(failure);
        } finally {
            TraceId.clear();
        }
    }
}

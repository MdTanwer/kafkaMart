package com.kafkamart.fraud;

import com.kafkamart.common.event.FraudAlert;
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
public class FraudAlertConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(FraudAlertConsumer.class);

    @Inject FraudAlertBuffer buffer;
    @Inject ServiceMetrics metrics;

    @Incoming("fraud-alerts-in")
    @Blocking
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public CompletionStage<Void> onAlert(Message<FraudAlert> message) {
        IncomingKafkaRecordMetadata<?, ?> metadata =
                message.getMetadata(IncomingKafkaRecordMetadata.class).orElse(null);
        if (metadata != null) {
            TraceId.restoreFrom(metadata.getHeaders());
        }
        FraudAlert alert = message.getPayload();
        try {
            buffer.add(alert);
            metrics.alert(alert.reason());
            LOG.info(
                    "alert buffered reason={} orderId={} userId={} size={}",
                    alert.reason(),
                    alert.orderId(),
                    alert.userId(),
                    buffer.size());
            return message.ack();
        } catch (RuntimeException failure) {
            LOG.error("failed to buffer fraud alert orderId={}", alert.orderId(), failure);
            return message.nack(failure);
        } finally {
            TraceId.clear();
        }
    }
}

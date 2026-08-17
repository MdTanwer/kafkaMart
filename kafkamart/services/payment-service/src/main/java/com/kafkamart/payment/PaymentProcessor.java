package com.kafkamart.payment;

import com.kafkamart.common.event.OrderCreated;
import com.kafkamart.common.event.PaymentCompleted;
import com.kafkamart.common.trace.TraceId;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.transactions.KafkaTransactions;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.OnOverflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consume {@code orders} → transform → produce {@code payments} in one Kafka transaction.
 *
 * <p>SmallRye 4.33 has no {@code @Incoming}+{@code @Outgoing} {@code exactly-once=true} shortcut
 * (verified on {@code KafkaConnectorIncomingConfiguration}). A plain processor would produce and
 * ack independently (at-least-once). The documented equivalent is {@link
 * KafkaTransactions#withTransactionAndAck}: the {@code PaymentCompleted} record and the consumed
 * offset are committed together, or both abort.
 */
@ApplicationScoped
public class PaymentProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(PaymentProcessor.class);

    @Inject PaymentGateway gateway;
    @Inject ChaosSwitch chaos;
    @Inject ServiceMetrics metrics;

    @Inject
    @Channel("payments-out")
    @OnOverflow(value = OnOverflow.Strategy.BUFFER, bufferSize = 500)
    KafkaTransactions<PaymentCompleted> paymentsOut;

    @Incoming("orders-in")
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public Uni<Void> process(Message<OrderCreated> incoming) {
        IncomingKafkaRecordMetadata<?, ?> metadata =
                incoming.getMetadata(IncomingKafkaRecordMetadata.class).orElse(null);
        if (metadata == null) {
            LOG.error("orders-in message missing IncomingKafkaRecordMetadata — nack");
            return Uni.createFrom()
                    .completionStage(
                            incoming.nack(new IllegalStateException("missing Kafka metadata")));
        }
        TraceId.restoreFrom(metadata.getHeaders());
        OrderCreated order = incoming.getPayload();
        if (order.traceId() != null) {
            TraceId.set(order.traceId());
        }
        metrics.consumed();
        // withTransactionAndAck acks on commit and nacks on abort without failing the stream.
        // incoming commit-strategy=ignore so the connector does not commit offsets itself.
        return paymentsOut
                .withTransactionAndAck(
                        incoming,
                        emitter -> {
                            PaymentCompleted payment = gateway.charge(order);
                            emitter.send(KafkaRecord.of(order.orderId(), payment));
                            // Crash AFTER the produce, BEFORE the TX commit → aborted records are
                            // invisible to isolation.level=read_committed. Redelivery writes once.
                            chaos.maybeCrash(order.orderId());
                            LOG.info(
                                    "payment TX orderId={} status={} key=orderId",
                                    order.orderId(),
                                    payment.status());
                            metrics.completed(payment.status());
                            return Uni.createFrom().voidItem();
                        })
                .eventually(TraceId::clear);
    }
}

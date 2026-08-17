package com.kafkamart.fraud;

import com.kafkamart.common.Topics;
import com.kafkamart.common.event.FraudAlert;
import com.kafkamart.common.event.OrderCreated;
import com.kafkamart.common.serde.JsonSerde;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stateless high-value filter plus stateful tumbling-window velocity count.
 *
 * <p><b>Why we do not {@code suppress()}:</b> fraud should fire on the 3rd in-window order, not
 * when the 5-minute window closes. {@code filter(count >= 3)} already drops counts 1 and 2. {@code
 * suppress(untilWindowCloses)} would delay the alert by up to window size + grace. Later increments
 * (4, 5, …) emit additional {@code VELOCITY} alerts on purpose — ongoing bursts stay visible.
 */
@ApplicationScoped
public class FraudTopology {
    private static final Logger LOG = LoggerFactory.getLogger(FraudTopology.class);

    static final String VELOCITY_STORE = "order-velocity-5m";
    static final String REPARTITION_NAME = "orders-by-user";
    static final String REASON_HIGH_VALUE = "HIGH_VALUE";
    static final String REASON_VELOCITY = "VELOCITY";
    static final BigDecimal HIGH_VALUE_THRESHOLD = new BigDecimal("10000");
    static final long VELOCITY_THRESHOLD = 3L;
    static final Duration WINDOW_SIZE = Duration.ofMinutes(5);
    static final Duration GRACE = Duration.ofMinutes(1);
    static final Duration RETENTION = Duration.ofMinutes(6);

    @Inject ServiceMetrics metrics;

    @Produces
    public Topology topology() {
        return build(metrics::consumed, metrics::produced);
    }

    static Topology build() {
        return build(() -> {}, () -> {});
    }

    static Topology build(Runnable onOrder, Runnable onAlert) {
        Serde<String> stringSerde = Serdes.String();
        Serde<OrderCreated> orderSerde = JsonSerde.of(OrderCreated.class);
        Serde<FraudAlert> alertSerde = JsonSerde.of(FraudAlert.class);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, OrderCreated> orders =
                builder.stream(
                                Topics.ORDERS,
                                Consumed.with(stringSerde, orderSerde)
                                        .withTimestampExtractor(
                                                new OrderCreatedTimestampExtractor())
                                        .withName("orders-source"))
                        .filter((key, order) -> order != null, Named.as("drop-null-orders"))
                        .peek((key, order) -> onOrder.run());

        // selectKey marks the key as changed → Streams creates a repartition topic before
        // groupByKey (even when order-api already keyed by userId). Teaching: changelog +
        // repartition topics appear in kafka-topics --list.
        KStream<String, OrderCreated> byUser =
                orders.selectKey((key, order) -> order.userId(), Named.as("select-user-id"));

        byUser.filter(
                        (userId, order) -> order.totalAmount().compareTo(HIGH_VALUE_THRESHOLD) > 0,
                        Named.as("high-value-filter"))
                .mapValues(FraudTopology::highValueAlert, Named.as("high-value-alert"))
                .peek(
                        (userId, alert) -> {
                            onAlert.run();
                            LOG.info(
                                    "FRAUD_ALERT reason={} orderId={} userId={} score={}",
                                    alert.reason(),
                                    alert.orderId(),
                                    alert.userId(),
                                    alert.score());
                        })
                .to(
                        Topics.FRAUD_ALERTS,
                        Produced.with(stringSerde, alertSerde).withName("high-value-sink"));

        byUser.groupByKey(
                        Grouped.<String, OrderCreated>as(REPARTITION_NAME)
                                .withKeySerde(stringSerde)
                                .withValueSerde(orderSerde))
                .windowedBy(TimeWindows.ofSizeAndGrace(WINDOW_SIZE, GRACE))
                .count(
                        Named.as("velocity-count"),
                        Materialized.<String, Long, WindowStore<Bytes, byte[]>>as(VELOCITY_STORE)
                                .withKeySerde(stringSerde)
                                .withValueSerde(Serdes.Long())
                                .withRetention(RETENTION))
                .toStream(Named.as("velocity-to-stream"))
                .filter(
                        (Windowed<String> windowed, Long count) ->
                                count != null && count >= VELOCITY_THRESHOLD,
                        Named.as("velocity-threshold"))
                .map(
                        (windowed, count) ->
                                KeyValue.pair(
                                        windowed.key(),
                                        velocityAlert(
                                                windowed.key(), count, windowed.window().start())),
                        Named.as("velocity-alert"))
                .peek(
                        (userId, alert) -> {
                            onAlert.run();
                            LOG.info(
                                    "FRAUD_ALERT reason={} orderId={} userId={} score={}",
                                    alert.reason(),
                                    alert.orderId(),
                                    alert.userId(),
                                    alert.score());
                        })
                .to(
                        Topics.FRAUD_ALERTS,
                        Produced.with(stringSerde, alertSerde).withName("velocity-sink"));

        return builder.build();
    }

    static FraudAlert highValueAlert(OrderCreated order) {
        return new FraudAlert(
                UUID.randomUUID(),
                Instant.now(),
                order.traceId(),
                order.orderId(),
                order.userId(),
                REASON_HIGH_VALUE,
                1.0);
    }

    static FraudAlert velocityAlert(String userId, long count, long windowStartMs) {
        String orderId = "velocity|" + userId + "|" + windowStartMs;
        double score = Math.min(1.0, count / 10.0);
        return FraudAlert.of(orderId, userId, REASON_VELOCITY, score);
    }
}

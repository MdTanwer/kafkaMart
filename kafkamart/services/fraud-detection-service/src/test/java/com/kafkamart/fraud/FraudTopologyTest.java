package com.kafkamart.fraud;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.kafkamart.common.Topics;
import com.kafkamart.common.event.FraudAlert;
import com.kafkamart.common.event.OrderCreated;
import com.kafkamart.common.event.OrderItem;
import com.kafkamart.common.serde.JsonSerde;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FraudTopologyTest {
    private static final Instant WINDOW_START = Instant.parse("2026-01-01T00:00:00Z");

    private TopologyTestDriver driver;
    private TestInputTopic<String, OrderCreated> orders;
    private TestOutputTopic<String, FraudAlert> alerts;
    private Serde<OrderCreated> orderSerde;
    private Serde<FraudAlert> alertSerde;

    @BeforeEach
    void startDriver() throws Exception {
        orderSerde = JsonSerde.of(OrderCreated.class);
        alertSerde = JsonSerde.of(FraudAlert.class);
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "fraud-topology-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class);
        props.put(
                StreamsConfig.STATE_DIR_CONFIG,
                Files.createTempDirectory("fraud-topology-test").toString());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);
        driver = new TopologyTestDriver(FraudTopology.build(), props);
        orders =
                driver.createInputTopic(
                        Topics.ORDERS, Serdes.String().serializer(), orderSerde.serializer());
        alerts =
                driver.createOutputTopic(
                        Topics.FRAUD_ALERTS,
                        Serdes.String().deserializer(),
                        alertSerde.deserializer());
    }

    @AfterEach
    void closeDriver() {
        if (driver != null) {
            driver.close();
        }
        if (orderSerde != null) {
            orderSerde.close();
        }
        if (alertSerde != null) {
            alertSerde.close();
        }
    }

    @Test
    void thirdOrderInTumblingWindowEmitsVelocityAlert() {
        String userId = "user-velocity";
        orders.pipeInput(userId, order("ord-1", userId, WINDOW_START, "10.00"));
        orders.pipeInput(userId, order("ord-2", userId, WINDOW_START.plusSeconds(10), "10.00"));
        assertTrue(alerts.isEmpty(), "counts 1 and 2 must not emit VELOCITY");

        orders.pipeInput(userId, order("ord-3", userId, WINDOW_START.plusSeconds(20), "10.00"));
        List<TestRecord<String, FraudAlert>> emitted = alerts.readRecordsToList();
        assertEquals(1, emitted.size());
        FraudAlert alert = emitted.get(0).getValue();
        assertEquals(userId, emitted.get(0).getKey());
        assertEquals(FraudTopology.REASON_VELOCITY, alert.reason());
        assertEquals(userId, alert.userId());
        assertTrue(alert.orderId().startsWith("velocity|" + userId + "|"));
    }

    @Test
    void ordersAcrossFiveMinuteBoundaryDoNotShareAWindow() {
        String userId = "user-boundary";
        Instant nextWindow = WINDOW_START.plus(Duration.ofMinutes(5));
        orders.pipeInput(userId, order("ord-a1", userId, WINDOW_START, "10.00"));
        orders.pipeInput(userId, order("ord-a2", userId, WINDOW_START.plusSeconds(30), "10.00"));
        orders.pipeInput(userId, order("ord-b1", userId, nextWindow, "10.00"));
        orders.pipeInput(userId, order("ord-b2", userId, nextWindow.plusSeconds(10), "10.00"));
        assertTrue(
                alerts.isEmpty(),
                "2 in window [00:00,00:05) + 2 in [00:05,00:10) must not emit VELOCITY");
    }

    @Test
    void amountAboveTenThousandEmitsHighValue() {
        String userId = "user-hv";
        orders.pipeInput(userId, order("ord-hv", userId, WINDOW_START, "10000.01"));
        List<TestRecord<String, FraudAlert>> emitted = alerts.readRecordsToList();
        assertEquals(1, emitted.size());
        assertEquals(FraudTopology.REASON_HIGH_VALUE, emitted.get(0).getValue().reason());
        assertEquals("ord-hv", emitted.get(0).getValue().orderId());
    }

    @Test
    void amountEqualToTenThousandIsNotHighValue() {
        String userId = "user-eq";
        orders.pipeInput(userId, order("ord-eq", userId, WINDOW_START, "10000.00"));
        assertTrue(alerts.isEmpty());
    }

    @Test
    void twoMinuteLateEventIsCountedTwentyMinuteLateIsDropped() {
        String countedUser = "user-late-in";
        String droppedUser = "user-late-out";
        Instant inWindow = WINDOW_START.plus(Duration.ofMinutes(3));
        Instant twoMinLate = inWindow.minus(Duration.ofMinutes(2));
        Instant withinGrace = WINDOW_START.plus(Duration.ofMinutes(5)).plusSeconds(30);
        Instant twentyMinLate = withinGrace.minus(Duration.ofMinutes(20));

        orders.pipeInput(countedUser, order("late-now-1", countedUser, inWindow, "10.00"));
        orders.pipeInput(
                countedUser, order("late-now-2", countedUser, inWindow.plusSeconds(1), "10.00"));
        assertTrue(alerts.isEmpty());
        // Stream time past window end (00:05) but inside grace (until 00:06).
        orders.pipeInput("clock", order("clock-1", "clock", withinGrace, "10.00"));
        orders.pipeInput(countedUser, order("late-2m", countedUser, twoMinLate, "10.00"));
        List<TestRecord<String, FraudAlert>> counted = alerts.readRecordsToList();
        assertEquals(
                1, counted.size(), "2 min late is within grace and must count toward velocity");
        assertEquals(FraudTopology.REASON_VELOCITY, counted.get(0).getValue().reason());

        orders.pipeInput(
                droppedUser, order("drop-now", droppedUser, withinGrace.plusSeconds(2), "10.00"));
        orders.pipeInput(droppedUser, order("drop-20m-1", droppedUser, twentyMinLate, "10.00"));
        orders.pipeInput(
                droppedUser,
                order("drop-20m-2", droppedUser, twentyMinLate.plusSeconds(1), "10.00"));
        orders.pipeInput(
                droppedUser,
                order("drop-20m-3", droppedUser, twentyMinLate.plusSeconds(2), "10.00"));
        assertTrue(
                alerts.isEmpty(),
                "events 20 min late are beyond grace+retention and must not form a velocity window");
    }

    private static OrderCreated order(
            String orderId, String userId, Instant occurredAt, String amount) {
        BigDecimal total = new BigDecimal(amount);
        return new OrderCreated(
                UUID.randomUUID(),
                occurredAt,
                "trace-" + orderId,
                orderId,
                userId,
                List.of(new OrderItem("SKU-1", 1, total)),
                total,
                "idem-" + orderId,
                "USD");
    }
}

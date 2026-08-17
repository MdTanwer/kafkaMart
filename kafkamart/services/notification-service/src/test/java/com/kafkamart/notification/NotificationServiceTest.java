package com.kafkamart.notification;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.kafkamart.common.event.PaymentCompleted;
import com.kafkamart.common.event.PaymentStatus;
import com.kafkamart.common.event.ShipmentCreated;
import com.kafkamart.common.test.AbstractKafkaDevServiceTest;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.common.annotation.Identifier;
import jakarta.inject.Inject;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

@QuarkusTest
class NotificationServiceTest extends AbstractKafkaDevServiceTest {
    @Inject NotificationSeedProducer seed;
    @Inject NotificationRecorder recorder;
    @Inject SlowChaos chaos;

    @Inject
    @Identifier("payments-email") EmailRebalanceListener emailRebalance;

    @Inject
    @Identifier("payments-sms") SmsRebalanceListener smsRebalance;

    @Test
    void bothGroupsReceiveTheSamePayment() {
        PaymentCompleted payment = payment("ord-fanout-" + UUID.randomUUID());
        seed.sendPayment(payment);
        awaitEmail(payment.eventId());
        awaitSms(payment.eventId());
        assertEquals(1, recorder.emailCount(payment.eventId()));
        assertEquals(1, recorder.smsCount(payment.eventId()));
        given().when()
                .get("/q/metrics")
                .then()
                .statusCode(200)
                .body(containsString("notifications_sent_total"))
                .body(containsString("channel=\"email\""))
                .body(containsString("channel=\"sms\""));
    }

    @Test
    void shipmentSendsTrackingLink() {
        ShipmentCreated shipment =
                ShipmentCreated.of(
                        "ship-" + UUID.randomUUID(),
                        "ord-track-" + UUID.randomUUID(),
                        "user-notify",
                        "CREATED");
        seed.sendShipment(shipment);
        Instant deadline = Instant.now().plusSeconds(20);
        while (Instant.now().isBefore(deadline)) {
            if (recorder.tracking().contains(shipment.eventId())) {
                return;
            }
            sleep(50);
        }
        throw new AssertionError("tracking link not recorded for " + shipment.eventId());
    }

    @Test
    void lagEndpointReturnsBothGroupsAndSaneOffsets() {
        PaymentCompleted payment = payment("ord-lag-" + UUID.randomUUID());
        seed.sendPayment(payment);
        awaitEmail(payment.eventId());
        awaitSms(payment.eventId());
        given().when()
                .get("/api/notify/lag")
                .then()
                .statusCode(200)
                .body("groups.size()", equalTo(2))
                .body("groups[0].groupId", equalTo("notification-email"))
                .body("groups[1].groupId", equalTo("notification-sms"))
                .body("groups[0].partitions.size()", greaterThanOrEqualTo(1))
                .body("groups[1].partitions.size()", greaterThanOrEqualTo(1))
                .body("groups[0].totalLag", greaterThanOrEqualTo(0))
                .body("groups[1].totalLag", greaterThanOrEqualTo(0));
    }

    @Test
    void slowChaosGrowsSmsLagThenKickRevokes() {
        chaos.setSleepMs(0);
        try {
            given().when()
                    .post("/api/notify/chaos/slow?ms=2500")
                    .then()
                    .statusCode(200)
                    .body("sleepMs", equalTo(2500));
            for (int i = 0; i < 6; i++) {
                seed.sendPayment(payment("ord-slow-" + i + "-" + UUID.randomUUID()));
            }
            long smsLag = awaitSmsLagAtLeast(1, Duration.ofSeconds(20));
            assertTrue(smsLag >= 1, "sms lag should grow while sleeping, was " + smsLag);

            chaos.setSleepMs(0);
            sleep(4000);

            int assignedBefore = countAssigned(smsRebalance.events());
            int revokedBefore = countRevoked(smsRebalance.events());
            chaos.setSleepMs(11000);
            seed.sendPayment(payment("ord-kick-" + UUID.randomUUID()));
            Instant deadline = Instant.now().plusSeconds(40);
            boolean kicked = false;
            while (Instant.now().isBefore(deadline)) {
                if (countRevoked(smsRebalance.events()) > revokedBefore
                        || countAssigned(smsRebalance.events()) > assignedBefore
                        || smsRebalance.events().stream()
                                .anyMatch(
                                        line -> line.contains("kicked") || line.contains("lost"))) {
                    kicked = true;
                    break;
                }
                sleep(200);
            }
            assertTrue(
                    kicked,
                    "max.poll.interval.ms=10000 should kick payments-sms during poll-thread sleep."
                            + " events="
                            + smsRebalance.events());
        } finally {
            chaos.setSleepMs(0);
        }
    }

    @Test
    void secondMemberShowsCooperativeVsEagerRevoke() {
        assertTrue(
                emailRebalance.events().stream().anyMatch(line -> line.contains("assigned")),
                "email cooperative listener must log startup assignment: "
                        + emailRebalance.events());
        assertTrue(
                smsRebalance.events().stream().anyMatch(line -> line.contains("assigned")),
                "sms eager listener must log startup assignment: " + smsRebalance.events());

        int emailRevokedBefore = countRevoked(emailRebalance.events());
        int smsRevokedBefore = countRevoked(smsRebalance.events());

        joinAndLeave("notification-email", CooperativeStickyAssignor.class.getName(), "payments");
        joinAndLeave("notification-sms", RangeAssignor.class.getName(), "payments");

        Instant deadline = Instant.now().plusSeconds(20);
        boolean emailMoved = false;
        boolean smsMoved = false;
        while (Instant.now().isBefore(deadline)) {
            emailMoved =
                    countRevoked(emailRebalance.events()) > emailRevokedBefore
                            || emailRebalance.events().size() > emailRevokedBefore + 1;
            smsMoved = countRevoked(smsRebalance.events()) > smsRevokedBefore;
            if (emailMoved && smsMoved) {
                break;
            }
            sleep(100);
        }
        assertTrue(
                smsMoved,
                "eager RangeAssignor must revoke on member join/leave: " + smsRebalance.events());
        assertTrue(
                emailRebalance.events().stream()
                        .anyMatch(line -> line.contains("cooperative-sticky")),
                emailRebalance.events().toString());
        assertTrue(
                smsRebalance.events().stream().anyMatch(line -> line.contains("eager-range")),
                smsRebalance.events().toString());
    }

    private void joinAndLeave(String groupId, String assignor, String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "rebalance-probe-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, assignor);
        try (KafkaConsumer<String, String> extra = new KafkaConsumer<>(props)) {
            extra.subscribe(List.of(topic));
            extra.poll(Duration.ofSeconds(8));
        }
    }

    private static int countAssigned(List<String> events) {
        int count = 0;
        for (String line : events) {
            if (line.contains("assigned")) {
                count++;
            }
        }
        return count;
    }

    private static int countRevoked(List<String> events) {
        int count = 0;
        for (String line : events) {
            if (line.contains("revoked")) {
                count++;
            }
        }
        return count;
    }

    private long awaitSmsLagAtLeast(long min, Duration timeout) {
        Instant deadline = Instant.now().plus(timeout);
        long last = -1;
        while (Instant.now().isBefore(deadline)) {
            last =
                    given().when()
                            .get("/api/notify/lag")
                            .then()
                            .statusCode(200)
                            .extract()
                            .jsonPath()
                            .getLong("groups.find { it.groupId == 'notification-sms' }.totalLag");
            if (last >= min) {
                return last;
            }
            sleep(150);
        }
        return last;
    }

    private void awaitEmail(UUID eventId) {
        awaitRecorded(eventId, true);
    }

    private void awaitSms(UUID eventId) {
        awaitRecorded(eventId, false);
    }

    private void awaitRecorded(UUID eventId, boolean email) {
        Instant deadline = Instant.now().plusSeconds(20);
        while (Instant.now().isBefore(deadline)) {
            boolean found =
                    email ? recorder.emails().contains(eventId) : recorder.sms().contains(eventId);
            if (found) {
                return;
            }
            sleep(50);
        }
        throw new AssertionError(
                (email ? "email" : "sms")
                        + " did not see eventId="
                        + eventId
                        + " emails="
                        + recorder.emails().size()
                        + " sms="
                        + recorder.sms().size());
    }

    private static PaymentCompleted payment(String orderId) {
        return PaymentCompleted.of(
                orderId,
                "user-notify",
                new BigDecimal("12.00"),
                PaymentStatus.PAID,
                "pay-" + orderId);
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted");
        }
    }
}

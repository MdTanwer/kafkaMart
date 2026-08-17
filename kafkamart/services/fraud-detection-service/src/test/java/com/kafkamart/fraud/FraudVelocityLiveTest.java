package com.kafkamart.fraud;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.kafkamart.common.event.FraudAlert;
import com.kafkamart.common.event.OrderCreated;
import com.kafkamart.common.event.OrderItem;
import com.kafkamart.common.test.AbstractKafkaDevServiceTest;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

@QuarkusTest
class FraudVelocityLiveTest extends AbstractKafkaDevServiceTest {
    @Inject OrderSeedProducer seed;

    @Test
    void threeOrdersInFiveMinutesProduceVelocityAlertOnRest() {
        waitUntilReady();
        String userId = "live-vel-" + UUID.randomUUID();
        Instant t0 = Instant.now();
        seed.send(order("ord-1-" + userId, userId, t0, "25.00"));
        seed.send(order("ord-2-" + userId, userId, t0.plusSeconds(1), "25.00"));
        seed.send(order("ord-3-" + userId, userId, t0.plusSeconds(2), "25.00"));

        FraudAlert alert = awaitVelocity(userId);
        assertEquals(FraudTopology.REASON_VELOCITY, alert.reason());
        assertEquals(userId, alert.userId());
        assertTrue(alert.orderId().contains(userId));
    }

    @Test
    void highValueOrderAppearsOnRest() {
        waitUntilReady();
        String userId = "live-hv-" + UUID.randomUUID();
        String orderId = "ord-hv-" + userId;
        seed.send(order(orderId, userId, Instant.now(), "10000.01"));
        FraudAlert alert = awaitHighValue(orderId);
        assertEquals(FraudTopology.REASON_HIGH_VALUE, alert.reason());
        assertEquals(userId, alert.userId());
    }

    private static void waitUntilReady() {
        Instant deadline = Instant.now().plusSeconds(45);
        while (Instant.now().isBefore(deadline)) {
            int status = given().when().get("/q/health/ready").then().extract().statusCode();
            if (status == 200) {
                return;
            }
            sleep(200);
        }
        throw new AssertionError("Kafka Streams not ready in 45s");
    }

    private static FraudAlert awaitVelocity(String userId) {
        Instant deadline = Instant.now().plusSeconds(30);
        while (Instant.now().isBefore(deadline)) {
            List<FraudAlert> alerts = readAlerts();
            for (FraudAlert alert : alerts) {
                if (FraudTopology.REASON_VELOCITY.equals(alert.reason())
                        && userId.equals(alert.userId())) {
                    return alert;
                }
            }
            sleep(100);
        }
        throw new AssertionError(
                "No VELOCITY alert for userId=" + userId + " alerts=" + readAlerts().size());
    }

    private static FraudAlert awaitHighValue(String orderId) {
        Instant deadline = Instant.now().plusSeconds(30);
        while (Instant.now().isBefore(deadline)) {
            List<FraudAlert> alerts = readAlerts();
            for (FraudAlert alert : alerts) {
                if (FraudTopology.REASON_HIGH_VALUE.equals(alert.reason())
                        && orderId.equals(alert.orderId())) {
                    return alert;
                }
            }
            sleep(100);
        }
        throw new AssertionError("No HIGH_VALUE alert for orderId=" + orderId);
    }

    @SuppressWarnings("unchecked")
    private static List<FraudAlert> readAlerts() {
        return given().when()
                .get("/api/fraud/alerts")
                .then()
                .statusCode(200)
                .extract()
                .jsonPath()
                .getList(".", FraudAlert.class);
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

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted", interrupted);
        }
    }
}

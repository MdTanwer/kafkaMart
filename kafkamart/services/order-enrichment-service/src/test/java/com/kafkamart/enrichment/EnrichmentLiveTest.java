package com.kafkamart.enrichment;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.kafkamart.common.event.EnrichedOrder;
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
class EnrichmentLiveTest extends AbstractKafkaDevServiceTest {
    @Inject EnrichmentSeedProducer seed;

    @Test
    void profileThenOrderIsQueryableAsEnrichedOrder() {
        waitUntilReady();
        String userId = "live-" + UUID.randomUUID();
        String orderId = "ord-" + userId;
        seed.sendUser(userId, "Ada Lovelace", "ada@kafkamart.dev");
        seed.sendOrder(order(orderId, userId));

        EnrichedOrder found = awaitIq(orderId);
        assertEquals(userId, found.userId());
        assertEquals("Ada Lovelace", found.userName());
        assertEquals("ada@kafkamart.dev", found.userEmail());
        assertEquals(orderId, found.orderId());
    }

    @Test
    void unknownOrderReturns404WithMetadataHost() {
        waitUntilReady();
        given().when()
                .get("/api/enrichment/orders/{orderId}", "missing-" + UUID.randomUUID())
                .then()
                .statusCode(404)
                .body("host", equalTo("0.0.0.0"))
                .body("port", equalTo(8087));
    }

    private static void waitUntilReady() {
        Instant deadline = Instant.now().plusSeconds(60);
        while (Instant.now().isBefore(deadline)) {
            int status = given().when().get("/q/health/ready").then().extract().statusCode();
            if (status == 200) {
                return;
            }
            sleep(200);
        }
        throw new AssertionError("Kafka Streams not ready in 60s");
    }

    private static EnrichedOrder awaitIq(String orderId) {
        Instant deadline = Instant.now().plusSeconds(30);
        while (Instant.now().isBefore(deadline)) {
            int status =
                    given().when()
                            .get("/api/enrichment/orders/{orderId}", orderId)
                            .then()
                            .extract()
                            .statusCode();
            if (status == 200) {
                return given().when()
                        .get("/api/enrichment/orders/{orderId}", orderId)
                        .then()
                        .statusCode(200)
                        .extract()
                        .as(EnrichedOrder.class);
            }
            sleep(100);
        }
        throw new AssertionError("IQ did not return EnrichedOrder for " + orderId);
    }

    private static OrderCreated order(String orderId, String userId) {
        return new OrderCreated(
                UUID.randomUUID(),
                Instant.now(),
                "trace-" + orderId,
                orderId,
                userId,
                List.of(new OrderItem("SKU-1", 1, new BigDecimal("9.99"))),
                new BigDecimal("9.99"),
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

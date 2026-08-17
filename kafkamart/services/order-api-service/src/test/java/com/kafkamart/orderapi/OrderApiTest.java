package com.kafkamart.orderapi;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.kafkamart.common.event.OrderCreated;
import com.kafkamart.common.test.AbstractKafkaDevServiceTest;
import com.kafkamart.common.trace.TraceId;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

@QuarkusTest
class OrderApiTest extends AbstractKafkaDevServiceTest {
    @Inject OrderCapture capture;

    @Test
    void postOrderProducesKeyedRecordWithTraceIdHeader() {
        String traceId = "trace-" + UUID.randomUUID();
        String userId = "user-ada";
        String orderId = postOrder(userId, "idem-" + UUID.randomUUID(), traceId);

        OrderCapture.Captured record = awaitOrder(orderId);
        assertEquals(userId, record.key());
        OrderCreated event = record.event();
        assertEquals(orderId, event.orderId());
        assertEquals(userId, event.userId());
        assertEquals(traceId, event.traceId());
        assertEquals(1, event.items().size());
        assertEquals("SKU-1", event.items().get(0).sku());
        assertEquals(2, event.items().get(0).quantity());
        assertNotNull(record.traceId(), "traceId Kafka header must be present");
        assertEquals(traceId, record.traceId());
    }

    @Test
    void idempotencyKeyReturnsStoredOrderIdWithoutSecondProduce() {
        String idempotencyKey = "idem-" + UUID.randomUUID();
        String first = postOrder("user-ada", idempotencyKey, "trace-" + UUID.randomUUID());
        String second = postOrder("user-ada", idempotencyKey, "trace-" + UUID.randomUUID());
        assertEquals(first, second);

        awaitOrder(first);
        Instant deadline = Instant.now().plusSeconds(2);
        while (Instant.now().isBefore(deadline)) {
            Thread.onSpinWait();
        }
        long count =
                capture.snapshot().stream()
                        .filter(c -> idempotencyKey.equals(c.event().idempotencyKey()))
                        .count();
        assertEquals(1, count, "duplicate Idempotency-Key must not emit a second record");
    }

    @Test
    void sameUserLandsInSamePartitionAndVipGoesToZero() {
        String userId = "user-same-" + UUID.randomUUID();
        String firstId =
                postOrder(userId, "idem-" + UUID.randomUUID(), "trace-" + UUID.randomUUID());
        String secondId =
                postOrder(userId, "idem-" + UUID.randomUUID(), "trace-" + UUID.randomUUID());
        int firstPartition = awaitOrder(firstId).partition();
        int secondPartition = awaitOrder(secondId).partition();
        assertEquals(
                firstPartition, secondPartition, "same userId must hash to the same partition");

        String vipOrder =
                postOrder("vip-ada", "idem-" + UUID.randomUUID(), "trace-" + UUID.randomUUID());
        assertEquals(0, awaitOrder(vipOrder).partition());
    }

    @Test
    void missingUserIdIsRejected() {
        given().contentType(ContentType.JSON)
                .body("{\"items\":[{\"sku\":\"SKU-1\",\"quantity\":1,\"price\":1.00}]}")
                .when()
                .post("/api/orders")
                .then()
                .statusCode(400);
    }

    @Test
    void simulateLoadFiresAcrossFakeUsers() {
        given().when()
                .get("/api/orders/simulate-load?count=3")
                .then()
                .statusCode(202)
                .body("count", equalTo(3))
                .body("orderIds", hasSize(3));
    }

    @Test
    void metricsExposeOrderProducedCounter() {
        postOrder("user-metrics", "idem-" + UUID.randomUUID(), "trace-" + UUID.randomUUID());
        given().when()
                .get("/q/metrics")
                .then()
                .statusCode(200)
                .body(containsString("orders_produced_total"))
                .body(containsString("result=\"success\""));
    }

    private String postOrder(String userId, String idempotencyKey, String traceId) {
        return given().contentType(ContentType.JSON)
                .header(TraceId.HEADER, traceId)
                .header("Idempotency-Key", idempotencyKey)
                .body(orderJson(userId))
                .when()
                .post("/api/orders")
                .then()
                .statusCode(202)
                .body("orderId", notNullValue())
                .extract()
                .path("orderId");
    }

    private static String orderJson(String userId) {
        return """
                {
                  "userId": "%s",
                  "items": [{"sku": "SKU-1", "quantity": 2, "price": 9.99}],
                  "currency": "USD"
                }
                """
                .formatted(userId);
    }

    private OrderCapture.Captured awaitOrder(String orderId) {
        Instant deadline = Instant.now().plusSeconds(20);
        while (Instant.now().isBefore(deadline)) {
            List<OrderCapture.Captured> snapshot = capture.snapshot();
            for (OrderCapture.Captured captured : snapshot) {
                if (orderId.equals(captured.event().orderId())) {
                    return captured;
                }
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError("Interrupted waiting for orderId=" + orderId);
            }
        }
        throw new AssertionError(
                "Did not consume OrderCreated for orderId="
                        + orderId
                        + " captured="
                        + capture.snapshot().size()
                        + " in "
                        + Duration.ofSeconds(20));
    }
}

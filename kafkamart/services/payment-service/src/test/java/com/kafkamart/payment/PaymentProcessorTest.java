package com.kafkamart.payment;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.kafkamart.common.event.OrderCreated;
import com.kafkamart.common.event.OrderItem;
import com.kafkamart.common.event.PaymentStatus;
import com.kafkamart.common.test.AbstractKafkaDevServiceTest;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

@QuarkusTest
class PaymentProcessorTest extends AbstractKafkaDevServiceTest {
    @Inject OrderSeedProducer seed;
    @Inject PaymentCapture capture;

    @Test
    void amountBelowTenThousandIsPaidWithOrderIdKey() {
        OrderCreated order = order("ord-paid-" + UUID.randomUUID(), new BigDecimal("99.50"));
        seed.send(order);
        PaymentCapture.Captured paid = awaitPayment(order.orderId());
        assertEquals(order.orderId(), paid.key());
        assertEquals(PaymentStatus.PAID, paid.event().status());
        assertEquals(PaymentGateway.transactionId(order.orderId()), paid.event().transactionId());
        assertEquals(order.userId(), paid.event().userId());
        assertEquals(0, paid.event().amount().compareTo(order.totalAmount()));
        assertEquals(1, capture.countByOrderId(order.orderId()));
    }

    @Test
    void amountAtLeastTenThousandIsFailed() {
        OrderCreated order = order("ord-fail-" + UUID.randomUUID(), new BigDecimal("10000"));
        seed.send(order);
        PaymentCapture.Captured failed = awaitPayment(order.orderId());
        assertEquals(PaymentStatus.FAILED, failed.event().status());
        assertEquals(order.orderId(), failed.key());
    }

    @Test
    void chaosCrashAbortsThenRetryCommitsExactlyOnePayment() {
        OrderCreated order = order("ord-chaos-" + UUID.randomUUID(), new BigDecimal("12.00"));
        given().when()
                .post("/api/payments/chaos/crash")
                .then()
                .statusCode(202)
                .body("armed", equalTo(true));
        seed.send(order);
        PaymentCapture.Captured committed = awaitPayment(order.orderId());
        assertEquals(PaymentStatus.PAID, committed.event().status());
        assertEquals(1, capture.countByOrderId(order.orderId()), "read_committed must see one row");
    }

    @Test
    void configExposesEosSettingsAndReadCommittedRequirement() {
        given().when()
                .get("/api/payments/config")
                .then()
                .statusCode(200)
                .body("incomingCommitStrategy", equalTo("ignore"))
                .body("incomingEnableAutoCommit", equalTo(false))
                .body("outgoingTransactionalId", containsString("payment-tx-"))
                .body("downstreamIsolationLevelRequired", equalTo("read_committed"))
                .body("eosPattern", containsString("withTransactionAndAck"));
    }

    @Test
    void chaosEndpointIsArmedInTestProfile() {
        given().when()
                .get("/api/payments/config")
                .then()
                .statusCode(200)
                .body("chaosEnabled", equalTo(true));
    }

    private static OrderCreated order(String orderId, BigDecimal amount) {
        return OrderCreated.of(
                orderId,
                "user-pay",
                List.of(new OrderItem("SKU-1", 1, amount)),
                amount,
                "idem-" + orderId,
                "USD");
    }

    private PaymentCapture.Captured awaitPayment(String orderId) {
        Instant deadline = Instant.now().plusSeconds(45);
        while (Instant.now().isBefore(deadline)) {
            for (PaymentCapture.Captured captured : capture.snapshot()) {
                if (orderId.equals(captured.event().orderId())) {
                    return captured;
                }
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted waiting for payment orderId=" + orderId);
            }
        }
        throw new AssertionError(
                "Did not capture PaymentCompleted for orderId="
                        + orderId
                        + " captured="
                        + capture.snapshot().size()
                        + " in "
                        + Duration.ofSeconds(45));
    }
}

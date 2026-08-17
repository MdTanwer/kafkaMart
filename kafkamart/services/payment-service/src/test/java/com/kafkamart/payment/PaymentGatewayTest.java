package com.kafkamart.payment;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.kafkamart.common.event.OrderCreated;
import com.kafkamart.common.event.OrderItem;
import com.kafkamart.common.event.PaymentStatus;
import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Test;

class PaymentGatewayTest {
    private final PaymentGateway gateway = new PaymentGateway();

    @Test
    void thresholdIsExclusive() {
        assertEquals(PaymentStatus.PAID, gateway.charge(order(new BigDecimal("9999.99"))).status());
        assertEquals(PaymentStatus.FAILED, gateway.charge(order(new BigDecimal("10000"))).status());
        assertEquals(
                PaymentStatus.FAILED, gateway.charge(order(new BigDecimal("10000.01"))).status());
    }

    @Test
    void transactionIdIsDeterministicForRetries() {
        String orderId = "ord-det-1";
        assertEquals(
                PaymentGateway.transactionId(orderId),
                gateway.charge(order(orderId, new BigDecimal("1.00"))).transactionId());
        assertEquals(
                PaymentGateway.transactionId(orderId),
                gateway.charge(order(orderId, new BigDecimal("1.00"))).transactionId());
    }

    private static OrderCreated order(BigDecimal amount) {
        return order("ord-" + amount.toPlainString(), amount);
    }

    private static OrderCreated order(String orderId, BigDecimal amount) {
        return OrderCreated.of(
                orderId,
                "user-gw",
                List.of(new OrderItem("SKU-1", 1, amount)),
                amount,
                "idem-" + orderId,
                "USD");
    }
}

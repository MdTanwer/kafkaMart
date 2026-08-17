package com.kafkamart.payment;

import com.kafkamart.common.event.OrderCreated;
import com.kafkamart.common.event.PaymentCompleted;
import com.kafkamart.common.event.PaymentStatus;
import jakarta.enterprise.context.ApplicationScoped;
import java.math.BigDecimal;

/**
 * Deterministic stand-in for a card gateway: amount strictly below {@value #PAID_BELOW} is {@link
 * PaymentStatus#PAID}, otherwise {@link PaymentStatus#FAILED}. {@code transactionId} is a function
 * of {@code orderId} so a retried transaction produces the same logical payment.
 */
@ApplicationScoped
public class PaymentGateway {
    static final BigDecimal PAID_BELOW = new BigDecimal("10000");

    public PaymentCompleted charge(OrderCreated order) {
        PaymentStatus status =
                order.totalAmount().compareTo(PAID_BELOW) < 0
                        ? PaymentStatus.PAID
                        : PaymentStatus.FAILED;
        return PaymentCompleted.of(
                order.orderId(),
                order.userId(),
                order.totalAmount(),
                status,
                transactionId(order.orderId()));
    }

    static String transactionId(String orderId) {
        return "pay-" + orderId;
    }
}

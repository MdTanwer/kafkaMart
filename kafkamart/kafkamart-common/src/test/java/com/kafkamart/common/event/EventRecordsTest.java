package com.kafkamart.common.event;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class EventRecordsTest {
    @AfterEach
    void clearMdc() {
        com.kafkamart.common.trace.TraceId.clear();
    }

    @Test
    void orderCreatedCarriesEnvelopeAndItems() {
        OrderCreated created =
                OrderCreated.of(
                        "ord-1",
                        "user-1",
                        List.of(new OrderItem("SKU-1", 2, new BigDecimal("9.99"))),
                        new BigDecimal("19.98"),
                        "idem-1",
                        "USD");
        assertNotNull(created.eventId());
        assertNotNull(created.occurredAt());
        assertNotNull(created.traceId());
        assertEquals("ord-1", created.orderId());

        EnrichedOrder enriched = EnrichedOrder.from(created, "Ada", "ada@kafkamart.local");
        assertEquals("Ada", enriched.userName());
        assertEquals(created.orderId(), enriched.orderId());
        assertEquals(created.eventId(), enriched.eventId());
    }

    @Test
    void paymentAndInventoryStatusesMatchContract() {
        PaymentCompleted paid =
                PaymentCompleted.of(
                        "ord-1", "user-1", new BigDecimal("19.98"), PaymentStatus.PAID, "txn-1");
        assertEquals(PaymentStatus.PAID, paid.status());
        InventoryReserved reserved =
                InventoryReserved.of("ord-1", "SKU-1", 2, InventoryStatus.RESERVED);
        assertEquals(InventoryStatus.RESERVED, reserved.status());
    }
}

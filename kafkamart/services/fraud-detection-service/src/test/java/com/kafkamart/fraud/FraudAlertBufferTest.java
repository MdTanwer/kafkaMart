package com.kafkamart.fraud;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.kafkamart.common.event.FraudAlert;
import org.junit.jupiter.api.Test;

class FraudAlertBufferTest {
    @Test
    void keepsOnlyTheLastHundred() {
        FraudAlertBuffer buffer = new FraudAlertBuffer();
        for (int i = 0; i < 105; i++) {
            buffer.add(FraudAlert.of("ord-" + i, "user-1", "HIGH_VALUE", 1.0));
        }
        assertEquals(FraudAlertBuffer.MAX_ALERTS, buffer.size());
        assertEquals("ord-5", buffer.snapshot().get(0).orderId());
        assertEquals("ord-104", buffer.snapshot().get(99).orderId());
    }
}

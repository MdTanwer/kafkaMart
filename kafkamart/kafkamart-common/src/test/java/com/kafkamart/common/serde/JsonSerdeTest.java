package com.kafkamart.common.serde;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.kafkamart.common.event.OrderCreated;
import com.kafkamart.common.event.OrderItem;
import java.math.BigDecimal;
import java.util.List;
import org.apache.kafka.common.serialization.Serde;
import org.junit.jupiter.api.Test;

class JsonSerdeTest {
    @Test
    void roundTripsOrderCreated() {
        OrderCreated original =
                OrderCreated.of(
                        "ord-1",
                        "user-1",
                        List.of(new OrderItem("SKU-1", 1, new BigDecimal("9.99"))),
                        new BigDecimal("9.99"),
                        "idem-1",
                        "USD");
        try (Serde<OrderCreated> serde = JsonSerde.of(OrderCreated.class)) {
            byte[] bytes = serde.serializer().serialize("orders", original);
            OrderCreated copy = serde.deserializer().deserialize("orders", bytes);
            assertNotNull(copy);
            assertEquals(original.orderId(), copy.orderId());
            assertEquals(original.userId(), copy.userId());
            assertEquals(0, original.totalAmount().compareTo(copy.totalAmount()));
            assertEquals(original.occurredAt(), copy.occurredAt());
        }
    }
}

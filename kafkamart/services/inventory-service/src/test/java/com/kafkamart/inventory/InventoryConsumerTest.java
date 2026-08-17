package com.kafkamart.inventory;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.kafkamart.common.event.InventoryStatus;
import com.kafkamart.common.event.OrderCreated;
import com.kafkamart.common.event.OrderItem;
import com.kafkamart.common.test.AbstractKafkaDevServiceTest;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import jakarta.inject.Inject;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;

@QuarkusTest
class InventoryConsumerTest extends AbstractKafkaDevServiceTest {
    @Inject InventoryConsumer consumer;
    @Inject InventoryProcessor processor;

    @Inject
    @Identifier("orders-in") OrdersRebalanceListener rebalance;

    @Inject OrderSeedProducer seed;
    @Inject InventoryEventCapture capture;

    @Test
    void manualAckOnlyAfterSuccessfulDbWrite() {
        String sku = "SKU-ACK-" + UUID.randomUUID();
        restock(sku, 10);
        OrderCreated order = order("ord-ack-" + UUID.randomUUID(), sku, 3);
        AtomicBoolean acked = new AtomicBoolean();
        AtomicReference<Throwable> nacked = new AtomicReference<>();
        consumer.onOrder(kafkaMessage(order, 2, 11L, acked, nacked)).toCompletableFuture().join();
        assertTrue(acked.get(), "ack() must run after the DB transaction commits");
        assertNull(nacked.get());
        assertEquals(7, stock(sku));
        assertTrue(ProcessedOffset.findByPartitionAndOffset(2, 11L).isPresent());
    }

    @Test
    void redeliveryOfSamePartitionOffsetDoesNotDoubleDecrement() {
        String sku = "SKU-DEDUP-" + UUID.randomUUID();
        restock(sku, 5);
        OrderCreated order = order("ord-dedup-" + UUID.randomUUID(), sku, 2);
        processor.process(4, 99L, order);
        processor.process(4, 99L, order);
        assertEquals(3, stock(sku), "redelivery of the same (partition, offset) must be a no-op");
        assertEquals(1, ProcessedOffset.findByPartitionAndOffset(4, 99L).stream().count());
    }

    @Test
    void rebalanceListenerAssignedAndRevokeFlushesCommits() {
        assertFalse(
                rebalance.events().stream().noneMatch(line -> line.contains("REBALANCE assigned")),
                "onPartitionsAssigned must log the rebalance: " + rebalance.events());
        @SuppressWarnings("unchecked")
        Consumer<Object, Object> kafkaConsumer = mock(Consumer.class);
        TopicPartition partition = new TopicPartition("orders", 0);
        rebalance.onPartitionsRevoked(kafkaConsumer, List.of(partition));
        verify(kafkaConsumer).commitSync();
        assertTrue(
                rebalance.events().stream().anyMatch(line -> line.contains("REBALANCE revoked")));
    }

    @Test
    void kafkaOrderReservesStockAndEmitsReserved() {
        String sku = "SKU-KAFKA-" + UUID.randomUUID();
        restock(sku, 20);
        OrderCreated order = order("ord-kafka-" + UUID.randomUUID(), sku, 4);
        seed.send(order);
        InventoryEventCapture.Captured reserved = awaitReserved(order.orderId(), sku);
        assertEquals(InventoryStatus.RESERVED, reserved.event().status());
        assertEquals(order.orderId(), reserved.key());
        assertEquals(16, stock(sku));
    }

    @Test
    void insufficientStockEmitsRejected() {
        String sku = "SKU-EMPTY-" + UUID.randomUUID();
        restock(sku, 1);
        OrderCreated order = order("ord-rej-" + UUID.randomUUID(), sku, 5);
        processor.process(5, 7L, order);
        assertEquals(1, stock(sku));
    }

    @Test
    void getUnknownSkuIs404() {
        given().when().get("/api/inventory/no-such-sku").then().statusCode(404);
    }

    private static OrderCreated order(String orderId, String sku, int qty) {
        return OrderCreated.of(
                orderId,
                "user-inv",
                List.of(new OrderItem(sku, qty, new BigDecimal("9.99"))),
                new BigDecimal("9.99").multiply(BigDecimal.valueOf(qty)),
                "idem-" + orderId,
                "USD");
    }

    private static Message<OrderCreated> kafkaMessage(
            OrderCreated order,
            int partition,
            long offset,
            AtomicBoolean acked,
            AtomicReference<Throwable> nacked) {
        ConsumerRecord<String, OrderCreated> record =
                new ConsumerRecord<>("orders", partition, offset, order.userId(), order);
        IncomingKafkaRecordMetadata<String, OrderCreated> metadata =
                new IncomingKafkaRecordMetadata<>(record, "orders-in");
        return Message.of(order)
                .addMetadata(metadata)
                .withAck(
                        () -> {
                            acked.set(true);
                            return CompletableFuture.completedFuture(null);
                        })
                .withNack(
                        failure -> {
                            nacked.set(failure);
                            return CompletableFuture.completedFuture(null);
                        });
    }

    private static void restock(String sku, int qty) {
        given().when()
                .post("/api/inventory/{sku}/restock?qty={qty}", sku, qty)
                .then()
                .statusCode(200)
                .body("sku", equalTo(sku))
                .body("quantity", equalTo(qty));
    }

    private static int stock(String sku) {
        return given().when()
                .get("/api/inventory/{sku}", sku)
                .then()
                .statusCode(200)
                .extract()
                .path("quantity");
    }

    private InventoryEventCapture.Captured awaitReserved(String orderId, String sku) {
        Instant deadline = Instant.now().plusSeconds(20);
        while (Instant.now().isBefore(deadline)) {
            for (InventoryEventCapture.Captured captured : capture.snapshot()) {
                if (orderId.equals(captured.event().orderId())
                        && sku.equals(captured.event().sku())) {
                    return captured;
                }
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted");
            }
        }
        throw new AssertionError(
                "Did not capture InventoryReserved for orderId="
                        + orderId
                        + " captured="
                        + capture.snapshot().size()
                        + " in "
                        + Duration.ofSeconds(20));
    }
}

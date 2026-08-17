package com.kafkamart.inventory;

import com.kafkamart.common.event.InventoryReserved;
import com.kafkamart.common.event.InventoryStatus;
import com.kafkamart.common.event.OrderCreated;
import com.kafkamart.common.event.OrderItem;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.PersistenceException;
import jakarta.transaction.Transactional;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class InventoryProcessor {
    public record ProcessResult(boolean skipped, List<InventoryReserved> events) {
        static ProcessResult duplicate() {
            return new ProcessResult(true, List.of());
        }

        static ProcessResult applied(List<InventoryReserved> events) {
            return new ProcessResult(false, List.copyOf(events));
        }
    }

    @Inject ServiceMetrics metrics;

    /**
     * Offset ledger + stock mutation in one DB transaction. A crash after commit and before Kafka
     * {@code ack()} redelivers the same (partition, offset); UNIQUE(partition, offset) makes that a
     * no-op.
     */
    @Transactional
    public ProcessResult process(int partition, long offset, OrderCreated order) {
        if (ProcessedOffset.findByPartitionAndOffset(partition, offset).isPresent()) {
            metrics.duplicate();
            return ProcessResult.duplicate();
        }
        try {
            ProcessedOffset.record(partition, offset, order.orderId());
            ProcessedOffset.flush();
        } catch (PersistenceException duplicateInsert) {
            metrics.duplicate();
            return ProcessResult.duplicate();
        }
        List<InventoryReserved> events = new ArrayList<>();
        for (OrderItem item : order.items()) {
            Optional<Stock> locked = Stock.lockBySku(item.sku());
            if (locked.isPresent() && locked.get().quantity >= item.quantity()) {
                locked.get().quantity -= item.quantity();
                events.add(
                        InventoryReserved.of(
                                order.orderId(),
                                item.sku(),
                                item.quantity(),
                                InventoryStatus.RESERVED));
                metrics.reserved();
            } else {
                events.add(
                        InventoryReserved.of(
                                order.orderId(),
                                item.sku(),
                                item.quantity(),
                                InventoryStatus.REJECTED));
                metrics.rejected();
            }
        }
        return ProcessResult.applied(events);
    }
}

package com.kafkamart.inventory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.arc.Unremovable;
import jakarta.inject.Singleton;

@Singleton
@Unremovable
public class ServiceMetrics {
    private final Counter eventsProduced;
    private final Counter eventsConsumed;
    private final Counter reserved;
    private final Counter rejected;
    private final Counter duplicates;
    private final Counter failed;

    public ServiceMetrics(MeterRegistry registry) {
        this.eventsProduced =
                Counter.builder("kafkamart.events.produced")
                        .description("Events produced by inventory-service")
                        .tag("service", "inventory-service")
                        .register(registry);
        this.eventsConsumed =
                Counter.builder("kafkamart.events.consumed")
                        .description("Events consumed by inventory-service")
                        .tag("service", "inventory-service")
                        .register(registry);
        this.reserved =
                Counter.builder("inventory.reserved")
                        .description("SKU lines reserved")
                        .register(registry);
        this.rejected =
                Counter.builder("inventory.rejected")
                        .description("SKU lines rejected for insufficient stock")
                        .register(registry);
        this.duplicates =
                Counter.builder("inventory.duplicates")
                        .description("Redelivered (partition, offset) pairs skipped")
                        .register(registry);
        this.failed =
                Counter.builder("inventory.failed")
                        .description("Order processing failures (nack)")
                        .register(registry);
        eventsProduced.increment();
        eventsConsumed.increment();
    }

    public void produced() {
        eventsProduced.increment();
    }

    public void consumed() {
        eventsConsumed.increment();
    }

    public void reserved() {
        reserved.increment();
    }

    public void rejected() {
        rejected.increment();
    }

    public void duplicate() {
        duplicates.increment();
    }

    public void failed() {
        failed.increment();
    }
}

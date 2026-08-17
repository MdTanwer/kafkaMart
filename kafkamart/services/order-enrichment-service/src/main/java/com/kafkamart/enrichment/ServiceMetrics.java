package com.kafkamart.enrichment;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.arc.Unremovable;
import jakarta.inject.Singleton;

@Singleton
@Unremovable
public class ServiceMetrics {
    private final MeterRegistry registry;
    private final Counter eventsProduced;
    private final Counter eventsConsumed;

    public ServiceMetrics(MeterRegistry registry) {
        this.registry = registry;
        this.eventsProduced =
                Counter.builder("kafkamart.events.produced")
                        .description("Events produced by order-enrichment-service")
                        .tag("service", "order-enrichment-service")
                        .register(registry);
        this.eventsConsumed =
                Counter.builder("kafkamart.events.consumed")
                        .description("Events consumed by order-enrichment-service")
                        .tag("service", "order-enrichment-service")
                        .register(registry);
    }

    public void produced() {
        eventsProduced.increment();
        Counter.builder("kafkamart.enrichment.joins")
                .description("Successful order-user joins")
                .tag("service", "order-enrichment-service")
                .register(registry)
                .increment();
    }

    public void consumed() {
        eventsConsumed.increment();
    }
}

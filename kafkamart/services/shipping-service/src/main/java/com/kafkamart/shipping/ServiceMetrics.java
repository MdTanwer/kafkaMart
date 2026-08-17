package com.kafkamart.shipping;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.inject.Singleton;

@Singleton
public class ServiceMetrics {
    private final Counter eventsProduced;
    private final Counter eventsConsumed;

    public ServiceMetrics(MeterRegistry registry) {
        this.eventsProduced = Counter.builder("kafkamart.events.produced")
                .description("Events produced by shipping-service")
                .tag("service", "shipping-service")
                .register(registry);
        this.eventsConsumed = Counter.builder("kafkamart.events.consumed")
                .description("Events consumed by shipping-service")
                .tag("service", "shipping-service")
                .register(registry);
    }

    public void produced() {
        eventsProduced.increment();
    }

    public void consumed() {
        eventsConsumed.increment();
    }
}

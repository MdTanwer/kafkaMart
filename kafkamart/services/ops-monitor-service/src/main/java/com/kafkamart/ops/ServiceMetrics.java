package com.kafkamart.ops;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.arc.Unremovable;
import jakarta.inject.Singleton;

@Singleton
@Unremovable
public class ServiceMetrics {
    private final Counter eventsProduced;
    private final Counter eventsConsumed;

    public ServiceMetrics(MeterRegistry registry) {
        this.eventsProduced =
                Counter.builder("kafkamart.events.produced")
                        .description("Events produced by ops-monitor-service")
                        .tag("service", "ops-monitor-service")
                        .register(registry);
        this.eventsConsumed =
                Counter.builder("kafkamart.events.consumed")
                        .description("Events consumed by ops-monitor-service")
                        .tag("service", "ops-monitor-service")
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
}

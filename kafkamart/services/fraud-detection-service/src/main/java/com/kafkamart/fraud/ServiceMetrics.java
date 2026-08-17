package com.kafkamart.fraud;

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
                        .description("Events produced by fraud-detection-service")
                        .tag("service", "fraud-detection-service")
                        .register(registry);
        this.eventsConsumed =
                Counter.builder("kafkamart.events.consumed")
                        .description("Events consumed by fraud-detection-service")
                        .tag("service", "fraud-detection-service")
                        .register(registry);
    }

    public void produced() {
        eventsProduced.increment();
    }

    public void consumed() {
        eventsConsumed.increment();
    }

    public void alert(String reason) {
        Counter.builder("kafkamart.fraud.alerts")
                .description("FraudAlert records observed by the REST reader")
                .tag("service", "fraud-detection-service")
                .tag("reason", reason == null ? "unknown" : reason)
                .register(registry)
                .increment();
        produced();
    }
}

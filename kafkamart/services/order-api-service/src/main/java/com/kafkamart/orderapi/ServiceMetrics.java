package com.kafkamart.orderapi;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.arc.Unremovable;
import jakarta.inject.Singleton;

@Singleton
@Unremovable
public class ServiceMetrics {
    private final Counter eventsProduced;
    private final Counter eventsConsumed;
    private final Counter ordersProducedSuccess;
    private final Counter ordersProducedFailure;

    public ServiceMetrics(MeterRegistry registry) {
        this.eventsProduced =
                Counter.builder("kafkamart.events.produced")
                        .description("Events produced by order-api-service")
                        .tag("service", "order-api-service")
                        .register(registry);
        this.eventsConsumed =
                Counter.builder("kafkamart.events.consumed")
                        .description("Events consumed by order-api-service")
                        .tag("service", "order-api-service")
                        .register(registry);
        this.ordersProducedSuccess =
                Counter.builder("orders.produced")
                        .description("OrderCreated records emitted to Kafka")
                        .tag("result", "success")
                        .register(registry);
        this.ordersProducedFailure =
                Counter.builder("orders.produced")
                        .description("OrderCreated records emitted to Kafka")
                        .tag("result", "failure")
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

    public void orderProduced(String result) {
        if ("failure".equals(result)) {
            ordersProducedFailure.increment();
        } else {
            ordersProducedSuccess.increment();
        }
        eventsProduced.increment();
    }
}

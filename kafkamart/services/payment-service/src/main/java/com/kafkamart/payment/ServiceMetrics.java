package com.kafkamart.payment;

import com.kafkamart.common.event.PaymentStatus;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.arc.Unremovable;
import jakarta.inject.Singleton;

@Singleton
@Unremovable
public class ServiceMetrics {
    private final Counter eventsProduced;
    private final Counter eventsConsumed;
    private final Counter paid;
    private final Counter failed;

    public ServiceMetrics(MeterRegistry registry) {
        this.eventsProduced =
                Counter.builder("kafkamart.events.produced")
                        .description("Events produced by payment-service")
                        .tag("service", "payment-service")
                        .register(registry);
        this.eventsConsumed =
                Counter.builder("kafkamart.events.consumed")
                        .description("Events consumed by payment-service")
                        .tag("service", "payment-service")
                        .register(registry);
        this.paid =
                Counter.builder("payments.completed")
                        .description("PaymentCompleted records committed")
                        .tag("status", "paid")
                        .register(registry);
        this.failed =
                Counter.builder("payments.completed")
                        .description("PaymentCompleted records committed")
                        .tag("status", "failed")
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

    public void completed(PaymentStatus status) {
        eventsProduced.increment();
        if (status == PaymentStatus.PAID) {
            paid.increment();
        } else {
            failed.increment();
        }
    }
}

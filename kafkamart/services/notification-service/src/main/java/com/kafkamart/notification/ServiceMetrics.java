package com.kafkamart.notification;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.arc.Unremovable;
import jakarta.inject.Singleton;

@Singleton
@Unremovable
public class ServiceMetrics {
    private final Counter eventsProduced;
    private final Counter eventsConsumed;
    private final Counter email;
    private final Counter sms;
    private final Counter tracking;
    private final Counter duplicates;

    public ServiceMetrics(MeterRegistry registry) {
        this.eventsProduced =
                Counter.builder("kafkamart.events.produced")
                        .description("Events produced by notification-service")
                        .tag("service", "notification-service")
                        .register(registry);
        this.eventsConsumed =
                Counter.builder("kafkamart.events.consumed")
                        .description("Events consumed by notification-service")
                        .tag("service", "notification-service")
                        .register(registry);
        this.email =
                Counter.builder("notifications.sent")
                        .description("Fake notifications sent")
                        .tag("channel", "email")
                        .register(registry);
        this.sms =
                Counter.builder("notifications.sent")
                        .description("Fake notifications sent")
                        .tag("channel", "sms")
                        .register(registry);
        this.tracking =
                Counter.builder("notifications.sent")
                        .description("Fake notifications sent")
                        .tag("channel", "tracking")
                        .register(registry);
        this.duplicates =
                Counter.builder("notifications.duplicates")
                        .description("Redelivered eventIds skipped")
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

    public void email() {
        eventsConsumed.increment();
        email.increment();
    }

    public void sms() {
        eventsConsumed.increment();
        sms.increment();
    }

    public void tracking() {
        eventsConsumed.increment();
        tracking.increment();
    }

    public void duplicate() {
        duplicates.increment();
    }
}

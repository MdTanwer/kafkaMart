package com.kafkamart.payment;

import jakarta.inject.Singleton;
import java.util.concurrent.atomic.AtomicBoolean;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Arms a one-shot crash inside the payment transaction (after the outgoing record is sent, before
 * the Kafka TX commits). Used by {@code POST /api/payments/chaos/crash} to demo abort + redelivery
 * without duplicates.
 */
@Singleton
public class ChaosSwitch {
    private static final Logger LOG = LoggerFactory.getLogger(ChaosSwitch.class);

    private final boolean enabled;
    private final AtomicBoolean armed = new AtomicBoolean(false);

    public ChaosSwitch(
            @ConfigProperty(name = "payment.chaos.enabled", defaultValue = "false")
                    boolean enabled) {
        this.enabled = enabled;
    }

    public boolean enabled() {
        return enabled;
    }

    public boolean armed() {
        return armed.get();
    }

    public void arm() {
        if (!enabled) {
            throw new IllegalStateException("chaos is disabled in this profile");
        }
        armed.set(true);
        LOG.warn("CHAOS armed — next payment TX will abort mid-processing");
    }

    public void maybeCrash(String orderId) {
        if (!enabled) {
            return;
        }
        if (armed.compareAndSet(true, false)) {
            LOG.warn("CHAOS crash mid-processing orderId={}", orderId);
            throw new ChaosCrashException("chaos crash mid-processing orderId=" + orderId);
        }
    }
}

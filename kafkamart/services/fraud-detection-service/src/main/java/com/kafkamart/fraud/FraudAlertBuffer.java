package com.kafkamart.fraud;

import com.kafkamart.common.event.FraudAlert;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayDeque;
import java.util.List;

/** Bounded in-memory projection of {@code fraud-alerts} for {@code GET /api/fraud/alerts}. */
@ApplicationScoped
public class FraudAlertBuffer {
    static final int MAX_ALERTS = 100;

    private final ArrayDeque<FraudAlert> alerts = new ArrayDeque<>();

    public synchronized void add(FraudAlert alert) {
        if (alert == null) {
            return;
        }
        alerts.addLast(alert);
        while (alerts.size() > MAX_ALERTS) {
            alerts.removeFirst();
        }
    }

    public synchronized List<FraudAlert> snapshot() {
        return List.copyOf(alerts);
    }

    public synchronized int size() {
        return alerts.size();
    }
}

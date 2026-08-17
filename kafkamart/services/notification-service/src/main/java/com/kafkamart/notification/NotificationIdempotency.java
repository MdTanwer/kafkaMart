package com.kafkamart.notification;

import jakarta.inject.Singleton;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Per-channel eventId set so a redelivered PaymentCompleted/ShipmentCreated is skip+ack. */
@Singleton
public class NotificationIdempotency {
    private final ConcurrentMap<String, Set<UUID>> seen = new ConcurrentHashMap<>();

    public boolean firstTime(String channel, UUID eventId) {
        if (eventId == null) {
            return true;
        }
        return seen.computeIfAbsent(channel, ignored -> ConcurrentHashMap.newKeySet()).add(eventId);
    }
}

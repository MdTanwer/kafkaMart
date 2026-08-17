package com.kafkamart.notification;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

/** Test-visible log of fake sends (also useful for the fan-out demo). */
@ApplicationScoped
public class NotificationRecorder {
    private final CopyOnWriteArrayList<UUID> emails = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<UUID> sms = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<UUID> tracking = new CopyOnWriteArrayList<>();

    public void email(UUID eventId) {
        emails.add(eventId);
    }

    public void sms(UUID eventId) {
        sms.add(eventId);
    }

    public void tracking(UUID eventId) {
        tracking.add(eventId);
    }

    public List<UUID> emails() {
        return List.copyOf(emails);
    }

    public List<UUID> sms() {
        return List.copyOf(sms);
    }

    public List<UUID> tracking() {
        return List.copyOf(tracking);
    }

    public long emailCount(UUID eventId) {
        return emails.stream().filter(eventId::equals).count();
    }

    public long smsCount(UUID eventId) {
        return sms.stream().filter(eventId::equals).count();
    }
}

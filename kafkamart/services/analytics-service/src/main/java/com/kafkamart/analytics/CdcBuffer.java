package com.kafkamart.analytics;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class CdcBuffer {
    static final int MAX = 50;

    private final ArrayDeque<Map<String, Object>> events = new ArrayDeque<>();

    public synchronized void add(Map<String, Object> event) {
        if (event == null) {
            return;
        }
        events.addLast(event);
        while (events.size() > MAX) {
            events.removeFirst();
        }
    }

    public synchronized List<Map<String, Object>> snapshot() {
        return List.copyOf(events);
    }
}

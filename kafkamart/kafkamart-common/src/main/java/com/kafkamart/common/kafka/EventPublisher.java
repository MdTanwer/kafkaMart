package com.kafkamart.common.kafka;

import org.springframework.kafka.core.KafkaTemplate;

/**
 * Shared producer wrapper. Header / idempotency behavior is implemented when that prompt arrives.
 */
public class EventPublisher {
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String serviceId;

    public EventPublisher(KafkaTemplate<String, Object> kafkaTemplate, String serviceId) {
        this.kafkaTemplate = kafkaTemplate;
        this.serviceId = serviceId;
    }

    public KafkaTemplate<String, Object> getKafkaTemplate() {
        return kafkaTemplate;
    }

    public String getServiceId() {
        return serviceId;
    }
}

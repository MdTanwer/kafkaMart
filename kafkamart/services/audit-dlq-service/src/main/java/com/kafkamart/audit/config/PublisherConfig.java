package com.kafkamart.audit.config;

import com.kafkamart.common.kafka.EventPublisher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

@Configuration
public class PublisherConfig {
    @Bean
    EventPublisher eventPublisher(KafkaTemplate<String, Object> kafkaTemplate,
                                  @Value("${kafkamart.service-id}") String serviceId) {
        return new EventPublisher(kafkaTemplate, serviceId);
    }
}

package com.kafkamart.common.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;

/**
 * Shared listener error handling / DLQ wiring — implemented when the DLQ prompt arrives.
 */
@Configuration
@EnableKafka
public class KafkaErrorHandlerConfig {
}

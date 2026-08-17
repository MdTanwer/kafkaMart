package com.kafkamart.audit;

import com.kafkamart.common.config.KafkaErrorHandlerConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@Import(KafkaErrorHandlerConfig.class)
public class AuditDlqApplication {
    public static void main(String[] args) {
        SpringApplication.run(AuditDlqApplication.class, args);
    }
}

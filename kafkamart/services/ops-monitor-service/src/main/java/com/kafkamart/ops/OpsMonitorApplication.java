package com.kafkamart.ops;

import com.kafkamart.common.config.KafkaErrorHandlerConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@Import(KafkaErrorHandlerConfig.class)
public class OpsMonitorApplication {
    public static void main(String[] args) {
        SpringApplication.run(OpsMonitorApplication.class, args);
    }
}

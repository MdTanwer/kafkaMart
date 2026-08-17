package com.kafkamart.inventory;

import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class InventoryHeartbeat {
    private static final Logger LOG = LoggerFactory.getLogger(InventoryHeartbeat.class);

    @Scheduled(every = "60s", identity = "inventory-stock-heartbeat")
    @Transactional
    void heartbeat() {
        LOG.info(
                "inventory heartbeat stockRows={} processedOffsets={}",
                Stock.count(),
                ProcessedOffset.count());
    }
}

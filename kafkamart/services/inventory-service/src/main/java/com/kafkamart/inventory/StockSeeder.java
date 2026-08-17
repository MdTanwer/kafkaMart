package com.kafkamart.inventory;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.transaction.Transactional;

@ApplicationScoped
public class StockSeeder {
    @Transactional
    void seed(@Observes StartupEvent event) {
        if (Stock.findBySku("SKU-1").isEmpty()) {
            Stock.restock("SKU-1", 100);
        }
        if (Stock.findBySku("SKU-LOAD").isEmpty()) {
            Stock.restock("SKU-LOAD", 1000);
        }
    }
}

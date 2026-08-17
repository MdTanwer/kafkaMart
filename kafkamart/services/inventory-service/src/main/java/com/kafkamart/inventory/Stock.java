package com.kafkamart.inventory;

import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.LockModeType;
import jakarta.persistence.Table;
import jakarta.transaction.Transactional;
import java.util.Optional;

@Entity
@Table(name = "stock")
public class Stock extends PanacheEntity {
    @Column(nullable = false, unique = true)
    public String sku;

    @Column(nullable = false)
    public int quantity;

    public static Optional<Stock> findBySku(String sku) {
        return find("sku", sku).firstResultOptional();
    }

    public static Optional<Stock> lockBySku(String sku) {
        return getEntityManager()
                .createQuery("from Stock s where s.sku = :sku", Stock.class)
                .setParameter("sku", sku)
                .setLockMode(LockModeType.PESSIMISTIC_WRITE)
                .getResultStream()
                .findFirst();
    }

    @Transactional
    public static Stock restock(String sku, int qty) {
        Stock stock =
                findBySku(sku)
                        .orElseGet(
                                () -> {
                                    Stock created = new Stock();
                                    created.sku = sku;
                                    created.quantity = 0;
                                    created.persist();
                                    return created;
                                });
        stock.quantity += qty;
        return stock;
    }
}

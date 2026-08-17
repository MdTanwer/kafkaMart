package com.kafkamart.inventory;

import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import java.util.Optional;

/**
 * Idempotency ledger for consumed Kafka records. UNIQUE(partition, offset) means a crash after the
 * DB commit but before {@code ack()} cannot decrement stock twice.
 */
@Entity
@Table(
        name = "processed_offset",
        uniqueConstraints =
                @UniqueConstraint(
                        name = "uk_processed_offset_partition_offset",
                        columnNames = {"partition_id", "record_offset"}))
public class ProcessedOffset extends PanacheEntity {
    @Column(name = "partition_id", nullable = false)
    public int partition;

    /**
     * Kafka record offset (column {@code record_offset}; {@code offset} is reserved in SQL/JPQL).
     */
    @Column(name = "record_offset", nullable = false)
    public long offset;

    @Column(name = "order_id", nullable = false)
    public String orderId;

    public static Optional<ProcessedOffset> findByPartitionAndOffset(int partition, long offset) {
        return find("partition = ?1 and offset = ?2", partition, offset).firstResultOptional();
    }

    public static ProcessedOffset record(int partition, long offset, String orderId) {
        ProcessedOffset row = new ProcessedOffset();
        row.partition = partition;
        row.offset = offset;
        row.orderId = orderId;
        row.persist();
        return row;
    }
}

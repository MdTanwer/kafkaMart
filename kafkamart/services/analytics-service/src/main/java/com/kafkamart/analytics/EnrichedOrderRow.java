package com.kafkamart.analytics;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.Instant;

@Entity
@Table(name = "enriched_orders")
public class EnrichedOrderRow extends PanacheEntityBase {
    @Id
    @Column(name = "orderId", nullable = false)
    public String orderId;

    @Column(name = "eventId")
    public String eventId;

    @Column(name = "occurredAt")
    public String occurredAt;

    @Column(name = "traceId")
    public String traceId;

    @Column(name = "userId")
    public String userId;

    @Column(name = "totalAmount")
    public BigDecimal totalAmount;

    @Column(name = "idempotencyKey")
    public String idempotencyKey;

    @Column(name = "currency")
    public String currency;

    @Column(name = "userName")
    public String userName;

    @Column(name = "userEmail")
    public String userEmail;

    @Column(name = "ingested_at")
    public Instant ingestedAt;
}

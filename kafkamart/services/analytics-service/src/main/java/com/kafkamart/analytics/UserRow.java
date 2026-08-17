package com.kafkamart.analytics;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "users")
public class UserRow extends PanacheEntityBase {
    @Id
    @Column(name = "user_id", nullable = false)
    public String userId;

    @Column(nullable = false)
    public String name;

    @Column(nullable = false)
    public String email;
}

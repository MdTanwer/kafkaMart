package com.kafkamart.orderapi.domain;

import com.kafkamart.common.event.OrderLine;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

public class Order {
    private String orderId;
    private String userId;
    private List<OrderLine> lines;
    private BigDecimal totalAmount;
    private String currency;
    private String paymentMethod;
    private OrderStatus status;
    private Instant createdAt;
    private String lastReason;

    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }
    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }
    public List<OrderLine> getLines() { return lines; }
    public void setLines(List<OrderLine> lines) { this.lines = lines; }
    public BigDecimal getTotalAmount() { return totalAmount; }
    public void setTotalAmount(BigDecimal totalAmount) { this.totalAmount = totalAmount; }
    public String getCurrency() { return currency; }
    public void setCurrency(String currency) { this.currency = currency; }
    public String getPaymentMethod() { return paymentMethod; }
    public void setPaymentMethod(String paymentMethod) { this.paymentMethod = paymentMethod; }
    public OrderStatus getStatus() { return status; }
    public void setStatus(OrderStatus status) { this.status = status; }
    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
    public String getLastReason() { return lastReason; }
    public void setLastReason(String lastReason) { this.lastReason = lastReason; }
}

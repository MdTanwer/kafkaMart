package com.kafkamart.common.event;

import java.math.BigDecimal;

/** Shared order line DTO — fields may be extended by later prompts. */
public class OrderLine {
    private String sku;
    private int quantity;
    private BigDecimal unitPrice;

    public String getSku() { return sku; }
    public void setSku(String sku) { this.sku = sku; }
    public int getQuantity() { return quantity; }
    public void setQuantity(int quantity) { this.quantity = quantity; }
    public BigDecimal getUnitPrice() { return unitPrice; }
    public void setUnitPrice(BigDecimal unitPrice) { this.unitPrice = unitPrice; }
}

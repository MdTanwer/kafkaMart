package com.kafkamart.orderapi;

import com.kafkamart.common.event.OrderCreated;
import com.kafkamart.common.event.OrderItem;
import com.kafkamart.orderapi.api.CreateOrderRequest;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class OrderService {
    static final List<String> LOAD_USERS = List.of("user-alice", "user-bob", "vip-carol");

    private final ConcurrentHashMap<String, String> idempotencyKeys = new ConcurrentHashMap<>();

    @Inject OrderProducer producer;

    public CompletionStage<String> place(CreateOrderRequest request, String idempotencyHeader) {
        String idempotencyKey =
                firstNonBlank(
                        idempotencyHeader, request.idempotencyKey(), UUID.randomUUID().toString());
        String existing = idempotencyKeys.get(idempotencyKey);
        if (existing != null) {
            return CompletableFuture.completedFuture(existing);
        }
        String orderId = UUID.randomUUID().toString();
        String winner = idempotencyKeys.putIfAbsent(idempotencyKey, orderId);
        if (winner != null) {
            return CompletableFuture.completedFuture(winner);
        }
        OrderCreated event =
                OrderCreated.of(
                        orderId,
                        request.userId(),
                        request.items(),
                        totalAmount(request),
                        idempotencyKey,
                        currency(request));
        return producer.send(event)
                .whenComplete(
                        (id, error) -> {
                            if (error != null) {
                                idempotencyKeys.remove(idempotencyKey, orderId);
                            }
                        });
    }

    public CompletionStage<List<String>> simulateLoad(int count) {
        if (count < 1) {
            throw new IllegalArgumentException("count must be >= 1");
        }
        List<CompletableFuture<String>> published = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String userId = LOAD_USERS.get(i % LOAD_USERS.size());
            CreateOrderRequest request =
                    new CreateOrderRequest(
                            userId,
                            List.of(new OrderItem("SKU-LOAD", 1, new BigDecimal("1.00"))),
                            new BigDecimal("1.00"),
                            "USD",
                            "load-" + UUID.randomUUID());
            published.add(place(request, request.idempotencyKey()).toCompletableFuture());
        }
        return CompletableFuture.allOf(published.toArray(CompletableFuture[]::new))
                .thenApply(unused -> published.stream().map(CompletableFuture::join).toList());
    }

    private static BigDecimal totalAmount(CreateOrderRequest request) {
        if (request.totalAmount() != null) {
            return request.totalAmount();
        }
        return request.items().stream()
                .map(item -> item.price().multiply(BigDecimal.valueOf(item.quantity())))
                .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    private static String currency(CreateOrderRequest request) {
        String currency = request.currency();
        return currency == null || currency.isBlank() ? "USD" : currency;
    }

    private static String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return UUID.randomUUID().toString();
    }
}

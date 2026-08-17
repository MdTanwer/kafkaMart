package com.kafkamart.payment;

/** Thrown from the payment TX after produce, before commit, when chaos is armed. */
public class ChaosCrashException extends RuntimeException {
    public ChaosCrashException(String message) {
        super(message);
    }
}

package com.kafkamart.payment.api;

/**
 * Effective Kafka / SmallRye settings for the EOS demo. Attribute names that SmallRye 4.33 does not
 * implement are still listed so the teaching prompt can be compared against reality.
 */
public record PaymentConfig(
        String incomingExactlyOnce,
        String incomingCommitStrategy,
        String incomingFailureStrategy,
        boolean incomingEnableAutoCommit,
        String outgoingTransactional,
        String outgoingTransactionalId,
        String eosPattern,
        String downstreamIsolationLevelRequired,
        String instanceId,
        boolean chaosEnabled) {}

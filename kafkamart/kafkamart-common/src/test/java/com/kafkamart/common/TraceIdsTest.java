package com.kafkamart.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class TraceIdsTest {
    @Test
    void currentOrNewAlwaysReturnsId() {
        assertNotNull(TraceIds.currentOrNew());
    }
}

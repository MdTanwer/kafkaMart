package com.kafkamart.notification;

import io.smallrye.reactive.messaging.kafka.KafkaClientService;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sleeps on the <em>Kafka poll thread</em> (not the {@code @Blocking} worker). SmallRye otherwise
 * keeps calling {@code poll()} while the worker sleeps, so {@code max.poll.interval.ms} would never
 * fire. Blocking the poll thread is what Kafka uses to kick a stuck member.
 */
@Singleton
public class SlowChaos {
    private static final Logger LOG = LoggerFactory.getLogger(SlowChaos.class);

    private final AtomicLong sleepMs = new AtomicLong(0);

    @Inject KafkaClientService kafka;

    public long sleepMs() {
        return sleepMs.get();
    }

    public void setSleepMs(long ms) {
        sleepMs.set(Math.max(0, ms));
        LOG.warn("CHAOS slow sms processing sleepMs={}", sleepMs.get());
    }

    public void maybeSlow() {
        long ms = sleepMs.get();
        if (ms <= 0) {
            return;
        }
        LOG.info("CHAOS slow sleeping {}ms on payments-sms poll thread", ms);
        kafka.getConsumer("payments-sms")
                .runOnPollingThread(
                        consumer -> {
                            try {
                                Thread.sleep(ms);
                            } catch (InterruptedException interrupted) {
                                Thread.currentThread().interrupt();
                                throw new IllegalStateException(
                                        "slow chaos interrupted", interrupted);
                            }
                        })
                .await()
                .atMost(Duration.ofMillis(ms + 15_000));
    }
}

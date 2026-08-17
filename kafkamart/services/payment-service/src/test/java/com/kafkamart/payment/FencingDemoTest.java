package com.kafkamart.payment;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.fail;

import com.kafkamart.common.test.AbstractKafkaDevServiceTest;
import io.quarkus.test.junit.QuarkusTest;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Zombie fencing: a second producer with the same {@code transactional.id} fences the first.
 * Application instances must use a unique {@code INSTANCE_ID} (see README).
 */
@QuarkusTest
class FencingDemoTest extends AbstractKafkaDevServiceTest {
    private static final Logger LOG = LoggerFactory.getLogger(FencingDemoTest.class);

    @Test
    void secondProducerWithSameTransactionalIdFencesTheFirst() throws Exception {
        String transactionalId = "fence-demo-" + UUID.randomUUID();
        try (KafkaProducer<String, String> first =
                        new KafkaProducer<>(producerProps(transactionalId, "first"));
                KafkaProducer<String, String> zombie =
                        new KafkaProducer<>(producerProps(transactionalId, "zombie"))) {
            first.initTransactions();
            first.beginTransaction();
            first.send(new ProducerRecord<>("payments", "fence-key", "{\"fence\":true}")).get();

            zombie.initTransactions();

            try {
                first.commitTransaction();
                fail("fenced producer must not commit");
            } catch (ProducerFencedException fenced) {
                logFence(transactionalId, fenced);
            } catch (RuntimeException failure) {
                Throwable cause = unwrap(failure);
                assertInstanceOf(
                        ProducerFencedException.class,
                        cause,
                        "expected ProducerFencedException, got " + failure);
                logFence(transactionalId, cause);
            }
        }
    }

    private static void logFence(String transactionalId, Throwable fenced) {
        LOG.warn(
                "ZOMBIE-FENCE transactional.id={} first producer fenced: {}",
                transactionalId,
                fenced.toString());
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current.getCause() != null && current.getCause() != current) {
            if (current instanceof ProducerFencedException) {
                return current;
            }
            if (current instanceof ExecutionException && current.getCause() != null) {
                current = current.getCause();
                continue;
            }
            if (current.getCause() instanceof ProducerFencedException) {
                return current.getCause();
            }
            current = current.getCause();
        }
        return current;
    }

    private Properties producerProps(String transactionalId, String role) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "fence-demo-" + role + "-" + UUID.randomUUID());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }
}

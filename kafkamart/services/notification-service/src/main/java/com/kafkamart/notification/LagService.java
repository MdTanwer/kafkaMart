package com.kafkamart.notification;

import com.kafkamart.notification.api.LagReport;
import com.kafkamart.notification.api.LagReport.GroupLag;
import com.kafkamart.notification.api.LagReport.PartitionLag;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consumer lag = log end offset − committed offset, via {@code listConsumerGroupOffsets} + {@code
 * listOffsets(OffsetSpec.latest())}.
 */
@ApplicationScoped
public class LagService {
    private static final Logger LOG = LoggerFactory.getLogger(LagService.class);
    static final String EMAIL_GROUP = "notification-email";
    static final String SMS_GROUP = "notification-sms";
    static final String EMAIL_ASSIGNOR =
            "org.apache.kafka.clients.consumer.CooperativeStickyAssignor";
    static final String SMS_ASSIGNOR = "org.apache.kafka.clients.consumer.RangeAssignor";

    private static final Duration ADMIN_TIMEOUT = Duration.ofSeconds(10);

    @ConfigProperty(name = "kafka.bootstrap.servers")
    String bootstrapServers;

    @ConfigProperty(name = "mp.messaging.incoming.payments-email.topic")
    String paymentsTopic;

    private AdminClient admin;

    @PostConstruct
    void open() {
        String bootstrap = stripListenerScheme(bootstrapServers);
        admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap));
        LOG.info("lag AdminClient bootstrap={}", bootstrap);
    }

    @PreDestroy
    void close() {
        if (admin != null) {
            admin.close(Duration.ofSeconds(5));
        }
    }

    public LagReport snapshot() {
        return new LagReport(
                List.of(
                        lagFor(EMAIL_GROUP, paymentsTopic, EMAIL_ASSIGNOR),
                        lagFor(SMS_GROUP, paymentsTopic, SMS_ASSIGNOR)));
    }

    GroupLag lagFor(String groupId, String topic, String assignor) {
        try {
            TopicDescription description =
                    admin.describeTopics(List.of(topic))
                            .allTopicNames()
                            .get(ADMIN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                            .get(topic);
            List<TopicPartition> partitions =
                    description.partitions().stream()
                            .map(info -> new TopicPartition(topic, info.partition()))
                            .toList();

            Map<TopicPartition, OffsetAndMetadata> committed =
                    admin.listConsumerGroupOffsets(groupId)
                            .partitionsToOffsetAndMetadata()
                            .get(ADMIN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            Map<TopicPartition, OffsetSpec> latestSpecs = new HashMap<>();
            Map<TopicPartition, OffsetSpec> earliestSpecs = new HashMap<>();
            for (TopicPartition partition : partitions) {
                latestSpecs.put(partition, OffsetSpec.latest());
                earliestSpecs.put(partition, OffsetSpec.earliest());
            }
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> ends =
                    admin.listOffsets(latestSpecs)
                            .all()
                            .get(ADMIN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> beginnings =
                    admin.listOffsets(earliestSpecs)
                            .all()
                            .get(ADMIN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            List<PartitionLag> rows = new ArrayList<>();
            long total = 0;
            for (TopicPartition partition : partitions) {
                long end = ends.get(partition).offset();
                OffsetAndMetadata committedMeta = committed.get(partition);
                long committedOffset =
                        committedMeta != null
                                ? committedMeta.offset()
                                : beginnings.get(partition).offset();
                long lag = Math.max(0, end - committedOffset);
                total += lag;
                rows.add(new PartitionLag(partition.partition(), committedOffset, end, lag));
            }
            rows.sort((a, b) -> Integer.compare(a.partition(), b.partition()));
            return new GroupLag(groupId, topic, assignor, total, rows);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "lag query interrupted for group " + groupId, interrupted);
        } catch (ExecutionException | TimeoutException failure) {
            throw new IllegalStateException("lag query failed for group " + groupId, failure);
        }
    }

    static String stripListenerScheme(String bootstrap) {
        if (bootstrap == null || bootstrap.isBlank()) {
            return "localhost:9092";
        }
        return bootstrap.replaceAll("(?i)[A-Za-z][A-Za-z0-9+.-]*://", "");
    }
}

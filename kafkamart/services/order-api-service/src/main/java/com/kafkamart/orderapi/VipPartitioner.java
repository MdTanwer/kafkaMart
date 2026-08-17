package com.kafkamart.orderapi;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Teaching partitioner: VIP users ({@code vip-*} keys) are pinned to partition 0. Everyone else is
 * hashed across the remaining partitions so a given {@code userId} always lands in the same
 * partition (ordering per user) without crowding the VIP lane.
 */
public final class VipPartitioner implements Partitioner {
    private static final Logger LOG = LoggerFactory.getLogger(VipPartitioner.class);
    static final String VIP_PREFIX = "vip-";

    @Override
    public int partition(
            String topic,
            Object key,
            byte[] keyBytes,
            Object value,
            byte[] valueBytes,
            Cluster cluster) {
        int numPartitions = partitionCount(topic, cluster);
        String keyString = stringify(key, keyBytes);
        int assigned;
        if (numPartitions <= 1) {
            assigned = 0;
        } else if (keyString.startsWith(VIP_PREFIX)) {
            assigned = 0;
        } else {
            byte[] bytes = keyBytes != null ? keyBytes : keyString.getBytes(StandardCharsets.UTF_8);
            int remaining = numPartitions - 1;
            assigned = 1 + (Utils.toPositive(Utils.murmur2(bytes)) % remaining);
        }
        LOG.debug("{} → {}", keyString, assigned);
        return assigned;
    }

    @Override
    public void close() {
        // no-op
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // no-op
    }

    private static int partitionCount(String topic, Cluster cluster) {
        Integer count = cluster.partitionCountForTopic(topic);
        if (count != null && count > 0) {
            return count;
        }
        return cluster.partitionsForTopic(topic).size();
    }

    private static String stringify(Object key, byte[] keyBytes) {
        if (key != null) {
            return key.toString();
        }
        if (keyBytes != null && keyBytes.length > 0) {
            return new String(keyBytes, StandardCharsets.UTF_8);
        }
        return "";
    }
}

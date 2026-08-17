package com.kafkamart.orderapi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.Test;

class VipPartitionerTest {
    private final VipPartitioner partitioner = new VipPartitioner();

    @Test
    void vipUsersAlwaysLandOnPartitionZero() {
        Cluster cluster = cluster("orders", 6);
        assertEquals(0, partition("vip-ada", cluster));
        assertEquals(0, partition("vip-bob", cluster));
    }

    @Test
    void sameUserHashesToTheSameNonVipPartition() {
        Cluster cluster = cluster("orders", 6);
        int first = partition("user-ada", cluster);
        int second = partition("user-ada", cluster);
        assertEquals(first, second);
        assertTrue(first >= 1 && first <= 5, "non-VIP users use remaining partitions 1..n-1");
    }

    @Test
    void differentUsersCanSpreadAcrossRemainingPartitions() {
        Cluster cluster = cluster("orders", 6);
        assertNotEquals(0, partition("user-ada", cluster));
        assertNotEquals(0, partition("user-bob", cluster));
    }

    @Test
    void singlePartitionTopicAlwaysReturnsZero() {
        Cluster cluster = cluster("orders", 1);
        assertEquals(0, partition("vip-ada", cluster));
        assertEquals(0, partition("user-ada", cluster));
    }

    private int partition(String key, Cluster cluster) {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        return partitioner.partition("orders", key, keyBytes, "value", null, cluster);
    }

    private static Cluster cluster(String topic, int partitions) {
        Node node = new Node(1, "localhost", 9092);
        List<PartitionInfo> infos = new ArrayList<>();
        for (int i = 0; i < partitions; i++) {
            infos.add(new PartitionInfo(topic, i, node, new Node[] {node}, new Node[] {node}));
        }
        return new Cluster("kafkamart-test", List.of(node), infos, Set.of(), Set.of());
    }
}

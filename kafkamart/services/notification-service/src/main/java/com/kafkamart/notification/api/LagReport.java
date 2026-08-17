package com.kafkamart.notification.api;

import java.util.List;

public record LagReport(List<GroupLag> groups) {
    public record GroupLag(
            String groupId,
            String topic,
            String assignor,
            long totalLag,
            List<PartitionLag> partitions) {}

    public record PartitionLag(int partition, long committedOffset, long endOffset, long lag) {}
}

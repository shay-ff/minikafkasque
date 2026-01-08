package com.kafka.jr.model;

import java.util.Objects;

/**
 * Consumer group offset tracking.
 * Represents a consumer in a group and which partition offsets it's at.
 */
public class ConsumerOffsetKey {
    private final String consumerGroup;
    private final String topic;
    private final int partition;

    public ConsumerOffsetKey(String consumerGroup, String topic, int partition) {
        this.consumerGroup = Objects.requireNonNull(consumerGroup);
        this.topic = Objects.requireNonNull(topic);
        this.partition = partition;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ConsumerOffsetKey)) return false;
        ConsumerOffsetKey that = (ConsumerOffsetKey) o;
        return partition == that.partition &&
               consumerGroup.equals(that.consumerGroup) &&
               topic.equals(that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(consumerGroup, topic, partition);
    }

    @Override
    public String toString() {
        return String.format("%s-%s-%d", consumerGroup, topic, partition);
    }
}

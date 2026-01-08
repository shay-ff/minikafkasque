package com.kafka.jr.model;

import java.util.Objects;

/**
 * Immutable topic-partition pair.
 * Used as a key in maps for thread-safe lookups.
 */
public class TopicPartition {
    private final String topic;
    private final int partition;

    public TopicPartition(String topic, int partition) {
        this.topic = Objects.requireNonNull(topic);
        this.partition = partition;
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
        if (!(o instanceof TopicPartition)) return false;
        TopicPartition that = (TopicPartition) o;
        return partition == that.partition && topic.equals(that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, partition);
    }

    @Override
    public String toString() {
        return topic + "-" + partition;
    }
}

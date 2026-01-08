package com.kafka.jr.consumer;

import com.kafka.jr.model.ConsumerOffsetKey;
import com.kafka.jr.model.TopicPartition;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Consumer group coordinator.
 * Manages:
 * 1. Consumer group membership
 * 2. Partition assignment (sticky assignment to minimize rebalancing)
 * 3. Offset tracking per consumer group per partition
 * 4. Rebalancing logic
 *
 * Thread-safe using locks and ConcurrentHashMap.
 */
public class ConsumerGroupCoordinator implements Closeable {
    private final Path offsetsFile;
    
    // Consumer group -> set of consumer IDs
    private final Map<String, Set<String>> groupMembers = new ConcurrentHashMap<>();
    
    // Consumer group -> topic -> set of partition IDs assigned to that group
    private final Map<String, Map<String, Set<Integer>>> groupPartitionAssignment = new ConcurrentHashMap<>();
    
    // Consumer -> (topic -> assigned partitions)
    private final Map<String, Map<String, Set<Integer>>> consumerAssignment = new ConcurrentHashMap<>();
    
    // Consumer offset key -> offset
    private final Map<ConsumerOffsetKey, Long> offsets = new ConcurrentHashMap<>();
    
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public ConsumerGroupCoordinator(Path dataDir) throws IOException {
        this.offsetsFile = dataDir.resolve("consumer_offsets.dat");
        Files.createDirectories(dataDir);
        loadOffsets();
    }

    /**
     * Join a consumer to a group.
     * Triggers rebalancing if this is a new consumer joining the group.
     * Returns the partition assignment for this consumer.
     */
    public Map<String, List<Integer>> joinGroup(String consumerGroup, String consumerId) {
        lock.writeLock().lock();
        try {
            Set<String> members = groupMembers.computeIfAbsent(consumerGroup, k -> ConcurrentHashMap.newKeySet());
            boolean isNewMember = members.add(consumerId);

            if (isNewMember) {
                System.out.println("Consumer " + consumerId + " joined group " + consumerGroup + 
                                 ". Rebalancing...");
                rebalanceGroup(consumerGroup);
            }

            // Return this consumer's assignment
            Map<String, Set<Integer>> myAssignment = consumerAssignment.get(consumerId);
            Map<String, List<Integer>> result = new HashMap<>();
            if (myAssignment != null) {
                for (Map.Entry<String, Set<Integer>> entry : myAssignment.entrySet()) {
                    result.put(entry.getKey(), new ArrayList<>(entry.getValue()));
                }
            }
            return result;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Leave a consumer from a group.
     * Triggers rebalancing.
     */
    public void leaveGroup(String consumerGroup, String consumerId) {
        lock.writeLock().lock();
        try {
            Set<String> members = groupMembers.get(consumerGroup);
            if (members != null && members.remove(consumerId)) {
                System.out.println("Consumer " + consumerId + " left group " + consumerGroup + 
                                 ". Rebalancing...");
                consumerAssignment.remove(consumerId);
                rebalanceGroup(consumerGroup);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Sticky partition assignment strategy.
     * Minimizes partition movement when consumers join/leave.
     * In a real system, this would be more sophisticated.
     */
    private void rebalanceGroup(String consumerGroup) {
        Set<String> members = groupMembers.get(consumerGroup);
        if (members == null || members.isEmpty()) {
            return;
        }

        Map<String, Set<Integer>> groupAssignment = groupPartitionAssignment
                .computeIfAbsent(consumerGroup, k -> new ConcurrentHashMap<>());

        // For each topic in this group, reassign partitions to consumers
        // Simple strategy: round-robin assignment
        for (String topic : groupAssignment.keySet()) {
            Set<Integer> partitions = groupAssignment.get(topic);
            List<String> consumerList = new ArrayList<>(members);
            Collections.sort(consumerList); // For deterministic ordering

            // Clear old assignments
            for (String consumer : consumerList) {
                Map<String, Set<Integer>> consumerTopics = consumerAssignment
                        .computeIfAbsent(consumer, k -> new ConcurrentHashMap<>());
                consumerTopics.put(topic, ConcurrentHashMap.newKeySet());
            }

            // Assign partitions round-robin
            int consumerIndex = 0;
            for (int partition : partitions) {
                String consumer = consumerList.get(consumerIndex % consumerList.size());
                Map<String, Set<Integer>> consumerTopics = consumerAssignment.get(consumer);
                consumerTopics.get(topic).add(partition);
                consumerIndex++;
            }
        }
    }

    /**
     * Register a topic-partition for a consumer group.
     * Called when a consumer in the group subscribes to a topic.
     */
    public void registerTopicPartitions(String consumerGroup, String topic, Set<Integer> partitions) {
        lock.writeLock().lock();
        try {
            Map<String, Set<Integer>> groupAssignment = groupPartitionAssignment
                    .computeIfAbsent(consumerGroup, k -> new ConcurrentHashMap<>());
            groupAssignment.put(topic, new HashSet<>(partitions));
            
            // Trigger rebalancing with existing members
            rebalanceGroup(consumerGroup);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Commit an offset for a consumer group on a partition.
     */
    public void commitOffset(String consumerGroup, String topic, int partition, long offset) {
        lock.writeLock().lock();
        try {
            ConsumerOffsetKey key = new ConsumerOffsetKey(consumerGroup, topic, partition);
            offsets.put(key, offset);
            saveOffsets(); // Persist immediately
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Get the committed offset for a consumer group on a partition.
     * Returns 0 if not set.
     */
    public long getCommittedOffset(String consumerGroup, String topic, int partition) {
        lock.readLock().lock();
        try {
            ConsumerOffsetKey key = new ConsumerOffsetKey(consumerGroup, topic, partition);
            return offsets.getOrDefault(key, 0L);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get all consumers in a group.
     */
    public Set<String> getGroupMembers(String consumerGroup) {
        Set<String> members = groupMembers.get(consumerGroup);
        return members != null ? new HashSet<>(members) : Collections.emptySet();
    }

    /**
     * Persist offsets to disk for crash recovery.
     */
    private void saveOffsets() {
        try (ObjectOutputStream oos = new ObjectOutputStream(
                new FileOutputStream(offsetsFile.toFile()))) {
            oos.writeObject(new HashMap<>(offsets));
        } catch (IOException e) {
            System.err.println("Failed to save offsets: " + e.getMessage());
        }
    }

    /**
     * Load offsets from disk on startup.
     */
    @SuppressWarnings("unchecked")
    private void loadOffsets() {
        if (!Files.exists(offsetsFile)) {
            return;
        }

        try (ObjectInputStream ois = new ObjectInputStream(
                new FileInputStream(offsetsFile.toFile()))) {
            Map<ConsumerOffsetKey, Long> loaded = (Map<ConsumerOffsetKey, Long>) ois.readObject();
            offsets.putAll(loaded);
            System.out.println("Loaded " + offsets.size() + " consumer offsets from disk");
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Failed to load offsets: " + e.getMessage());
        }
    }

    @Override
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            saveOffsets();
        } finally {
            lock.writeLock().unlock();
        }
    }
}

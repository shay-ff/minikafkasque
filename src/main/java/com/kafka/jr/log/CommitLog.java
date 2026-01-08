package com.kafka.jr.log;

import com.kafka.jr.model.Message;
import com.kafka.jr.model.TopicPartition;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Commit Log for a single topic partition.
 * Manages multiple log segments with rotation based on max segment size.
 * Thread-safe using ReadWriteLock for reads and exclusive lock for writes.
 *
 * Crash Recovery Strategy:
 * 1. On startup, scan all segment files for a partition
 * 2. For each segment, replay all messages and recover the max offset
 * 3. Track idempotent writes to prevent duplicate processing
 * 4. Resume writes to the currently active segment
 */
public class CommitLog implements Closeable {
    private static final long MAX_SEGMENT_SIZE = 100 * 1024 * 1024; // 100MB
    private static final String SEGMENT_NAME_FORMAT = "%020d.log";

    private final Path logDir;
    private final TopicPartition topicPartition;
    private final List<LogSegment> segments = new ArrayList<>();
    private volatile LogSegment activeSegment;
    private volatile long nextOffset = 0L;
    
    // Track idempotent writes to prevent duplicates
    private final Map<String, Long> idempotencyMap = new ConcurrentHashMap<>();
    
    // Thread-safe coordination: exclusive write lock, shared read lock
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public CommitLog(Path baseDir, String topic, int partition) throws IOException {
        this.logDir = baseDir.resolve(topic).resolve(String.valueOf(partition));
        this.topicPartition = new TopicPartition(topic, partition);
        
        Files.createDirectories(logDir);
        recover();
    }

    /**
     * Recover from disk on startup.
     * Loads all segment files and replays them to recover the max offset.
     */
    private void recover() throws IOException {
        lock.writeLock().lock();
        try {
            segments.clear();
            idempotencyMap.clear();

            // List all .log files in the directory, sorted by base offset
            File[] files = logDir.toFile().listFiles((dir, name) -> name.endsWith(".log"));
            if (files == null) {
                files = new File[0];
            }

            Arrays.sort(files, (a, b) -> {
                long offsetA = extractBaseOffset(a.getName());
                long offsetB = extractBaseOffset(b.getName());
                return Long.compare(offsetA, offsetB);
            });

            // Load each segment and recover offsets
            for (File file : files) {
                long baseOffset = extractBaseOffset(file.getName());
                LogSegment segment = new LogSegment(file.toPath(), baseOffset);
                segments.add(segment);

                // Recover idempotency map and max offset
                List<ByteBuffer> messages = segment.readFrom(baseOffset, Integer.MAX_VALUE);
                for (ByteBuffer msgBuffer : messages) {
                    Message msg = Message.deserialize(msgBuffer);
                    nextOffset = Math.max(nextOffset, msg.getOffset() + 1);
                    if (msg.getIdempotencyKey() != null) {
                        idempotencyMap.put(msg.getIdempotencyKey(), msg.getOffset());
                    }
                }
            }

            // Open or create active segment
            if (segments.isEmpty()) {
                activeSegment = new LogSegment(logDir.resolve(String.format(SEGMENT_NAME_FORMAT, 0L)), 0L);
                segments.add(activeSegment);
                nextOffset = 0L;
            } else {
                activeSegment = segments.get(segments.size() - 1);
                // nextOffset already set above
            }

            System.out.println("Recovered partition " + topicPartition + ": nextOffset=" + nextOffset + 
                             ", segments=" + segments.size());
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Append a message to the log.
     * Handles segment rotation and idempotent deduplication.
     * Returns the offset of the appended message, or -1 if it was deduplicated.
     */
    public long append(Message message) throws IOException {
        lock.writeLock().lock();
        try {
            // Check for idempotent duplicate
            if (message.getIdempotencyKey() != null) {
                Long existingOffset = idempotencyMap.get(message.getIdempotencyKey());
                if (existingOffset != null) {
                    // Already written, return existing offset
                    return existingOffset;
                }
            }

            // Check if we need to rotate to a new segment
            if (activeSegment.isFull(MAX_SEGMENT_SIZE)) {
                activeSegment.close();
                long newBaseOffset = nextOffset;
                activeSegment = new LogSegment(
                    logDir.resolve(String.format(SEGMENT_NAME_FORMAT, newBaseOffset)),
                    newBaseOffset
                );
                segments.add(activeSegment);
                System.out.println("Rotated segment for " + topicPartition + 
                                 " at offset " + newBaseOffset);
            }

            // Create message with assigned offset
            Message recordedMessage = new Message(
                message.getTopic(),
                message.getPartition(),
                nextOffset,
                System.currentTimeMillis(),
                message.getKey(),
                message.getValue(),
                message.getIdempotencyKey()
            );

            // Serialize and append
            ByteBuffer serialized = recordedMessage.serialize();
            activeSegment.append(serialized);
            activeSegment.fsync();

            // Track idempotency
            if (recordedMessage.getIdempotencyKey() != null) {
                idempotencyMap.put(recordedMessage.getIdempotencyKey(), nextOffset);
            }

            long assignedOffset = nextOffset;
            nextOffset++;
            return assignedOffset;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Read messages starting from a given offset.
     * Thread-safe read operation.
     */
    public List<Message> read(long startOffset, int maxMessages) throws IOException {
        lock.readLock().lock();
        try {
            List<Message> messages = new ArrayList<>();
            int count = 0;

            for (LogSegment segment : segments) {
                if (count >= maxMessages) break;

                if (startOffset <= segment.getMaxOffset()) {
                    List<ByteBuffer> buffers = segment.readFrom(startOffset, maxMessages - count);
                    for (ByteBuffer buf : buffers) {
                        messages.add(Message.deserialize(buf));
                        count++;
                    }
                }
            }

            return messages;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get the current next offset (where the next message will be written).
     */
    public long getNextOffset() {
        lock.readLock().lock();
        try {
            return nextOffset;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get the max offset that has been written.
     */
    public long getMaxOffset() {
        lock.readLock().lock();
        try {
            return nextOffset - 1;
        } finally {
            lock.readLock().unlock();
        }
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }

    private long extractBaseOffset(String filename) {
        return Long.parseLong(filename.replace(".log", ""));
    }

    @Override
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            for (LogSegment segment : segments) {
                segment.close();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public String toString() {
        return String.format("CommitLog{%s, nextOffset=%d, segments=%d}",
                topicPartition, nextOffset, segments.size());
    }
}

package com.kafka.jr.log;

import com.kafka.jr.model.TopicPartition;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Central log storage manager.
 * Maintains commit logs for all topic-partition pairs.
 * Thread-safe using ConcurrentHashMap and per-partition locks.
 */
public class LogStore implements Closeable {
    private final Path baseDir;
    private final Map<TopicPartition, CommitLog> logs = new ConcurrentHashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public LogStore(Path baseDir) throws IOException {
        this.baseDir = baseDir;
        Files.createDirectories(baseDir);
    }

    /**
     * Get or create a commit log for a topic-partition.
     * Lazy initialization: creates on first access.
     */
    public CommitLog getOrCreateLog(String topic, int partition) throws IOException {
        TopicPartition tp = new TopicPartition(topic, partition);
        
        CommitLog log = logs.get(tp);
        if (log != null) {
            return log;
        }

        lock.writeLock().lock();
        try {
            // Double-check after acquiring lock
            log = logs.get(tp);
            if (log != null) {
                return log;
            }

            log = new CommitLog(baseDir, topic, partition);
            logs.put(tp, log);
            return log;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Get an existing log (returns null if not found).
     */
    public CommitLog getLog(String topic, int partition) {
        return logs.get(new TopicPartition(topic, partition));
    }

    /**
     * Get all existing logs.
     */
    public Collection<CommitLog> getAllLogs() {
        return new ArrayList<>(logs.values());
    }

    @Override
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            IOException lastException = null;
            for (CommitLog log : logs.values()) {
                try {
                    log.close();
                } catch (IOException e) {
                    lastException = e;
                }
            }
            logs.clear();
            if (lastException != null) {
                throw lastException;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
}

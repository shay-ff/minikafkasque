package com.kafka.jr.log;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;

/**
 * Single segment file for WAL.
 * Immutable once closed. Used for reading.
 * Thread-safe for concurrent reads.
 */
public class LogSegment implements Closeable {
    private final Path filePath;
    private final long baseOffset; // First offset in this segment
    private final RandomAccessFile randomAccessFile;
    private final FileChannel fileChannel;
    private volatile long maxOffset; // Last offset written to this segment

    public LogSegment(Path filePath, long baseOffset) throws IOException {
        this.filePath = filePath;
        this.baseOffset = baseOffset;
        this.randomAccessFile = new RandomAccessFile(filePath.toFile(), "rw");
        this.fileChannel = randomAccessFile.getChannel();
        this.maxOffset = baseOffset - 1; // Start before first offset

        // If file already exists, recover max offset
        if (filePath.toFile().length() > 0) {
            recoverMaxOffset();
        }
    }

    /**
     * Scan file to find the last valid offset (used on startup).
     * This is crash recovery: we read all messages in the segment and track the max offset.
     */
    private void recoverMaxOffset() throws IOException {
        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
        long position = 0;
        
        while (position < fileChannel.size()) {
            sizeBuffer.clear();
            int bytesRead = fileChannel.read(sizeBuffer, position);
            
            if (bytesRead < 4) {
                // Partial write at end of file (crash recovery)
                break;
            }
            
            sizeBuffer.flip();
            int messageSize = sizeBuffer.getInt();
            
            if (messageSize <= 0 || position + 4 + messageSize > fileChannel.size()) {
                // Corrupted message at end (crash recovery)
                break;
            }
            
            try {
                ByteBuffer messageBuffer = ByteBuffer.allocate(messageSize);
                fileChannel.read(messageBuffer, position + 4);
                messageBuffer.flip();
                
                // Deserialize to get offset
                byte[] topicBytes = new byte[messageBuffer.getInt()];
                messageBuffer.get(topicBytes);
                messageBuffer.getInt(); // partition
                long offset = messageBuffer.getLong();
                
                maxOffset = offset;
                position += 4 + messageSize;
            } catch (Exception e) {
                // Corrupted message, stop recovery
                break;
            }
        }
    }

    /**
     * Append a message to this segment.
     * NOT thread-safe for writes - must be called with synchronization.
     */
    public synchronized void append(ByteBuffer messageBuffer) throws IOException {
        messageBuffer.flip();
        byte[] data = new byte[messageBuffer.remaining()];
        messageBuffer.get(data);

        // Write size prefix followed by message
        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
        sizeBuffer.putInt(data.length);
        sizeBuffer.flip();

        fileChannel.write(sizeBuffer);
        fileChannel.write(ByteBuffer.wrap(data));

        // Update maxOffset based on message
        ByteBuffer tempBuffer = ByteBuffer.wrap(data);
        tempBuffer.getInt(); // topic length
        tempBuffer.getInt(); // skip topic bytes
        tempBuffer.getInt(); // partition
        long offset = tempBuffer.getLong();
        this.maxOffset = offset;
    }

    /**
     * Sync to disk - ensures message is persisted.
     */
    public synchronized void fsync() throws IOException {
        fileChannel.force(true);
    }

    /**
     * Read a message at a specific offset from this segment.
     * Thread-safe for reads.
     */
    public Optional<ByteBuffer> readAt(long offset) throws IOException {
        if (offset < baseOffset || offset > maxOffset) {
            return Optional.empty();
        }

        synchronized (this) {
            ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
            long position = 0;
            long currentOffset = baseOffset;

            while (position < fileChannel.size() && currentOffset <= offset) {
                sizeBuffer.clear();
                int bytesRead = fileChannel.read(sizeBuffer, position);
                if (bytesRead < 4) break;

                sizeBuffer.flip();
                int messageSize = sizeBuffer.getInt();

                if (currentOffset == offset) {
                    ByteBuffer messageBuffer = ByteBuffer.allocate(messageSize);
                    fileChannel.read(messageBuffer, position + 4);
                    messageBuffer.flip();
                    return Optional.of(messageBuffer);
                }

                position += 4 + messageSize;
                currentOffset++;
            }
        }
        return Optional.empty();
    }

    /**
     * Read all messages starting from a given offset up to maxMessages.
     */
    public List<ByteBuffer> readFrom(long startOffset, int maxMessages) throws IOException {
        List<ByteBuffer> messages = new ArrayList<>();
        if (startOffset > maxOffset) {
            return messages;
        }

        long offset = Math.max(startOffset, baseOffset);
        int count = 0;

        synchronized (this) {
            ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
            long position = 0;
            long currentOffset = baseOffset;

            while (position < fileChannel.size() && count < maxMessages) {
                sizeBuffer.clear();
                int bytesRead = fileChannel.read(sizeBuffer, position);
                if (bytesRead < 4) break;

                sizeBuffer.flip();
                int messageSize = sizeBuffer.getInt();

                if (messageSize <= 0 || position + 4 + messageSize > fileChannel.size()) {
                    break; // Corrupted or partial message
                }

                if (currentOffset >= offset) {
                    ByteBuffer messageBuffer = ByteBuffer.allocate(messageSize);
                    fileChannel.read(messageBuffer, position + 4);
                    messageBuffer.flip();
                    messages.add(messageBuffer);
                    count++;
                }

                position += 4 + messageSize;
                currentOffset++;
            }
        }
        return messages;
    }

    public long getBaseOffset() {
        return baseOffset;
    }

    public long getMaxOffset() {
        return maxOffset;
    }

    public boolean isFull(long maxSegmentSize) {
        try {
            return fileChannel.size() >= maxSegmentSize;
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public void close() throws IOException {
        fileChannel.close();
        randomAccessFile.close();
    }

    @Override
    public String toString() {
        return String.format("LogSegment{file=%s, baseOffset=%d, maxOffset=%d}",
                filePath.getFileName(), baseOffset, maxOffset);
    }
}

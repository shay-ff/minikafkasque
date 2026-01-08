package com.kafka.jr.producer;

import com.kafka.jr.log.LogStore;
import com.kafka.jr.model.Message;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * HTTP handler for producer operations.
 * Endpoint: POST /produce
 *
 * Request body (JSON):
 * {
 *   "topic": "my-topic",
 *   "key": "base64-encoded-key",
 *   "value": "base64-encoded-value",
 *   "idempotency_key": "unique-id"
 * }
 *
 * Response (JSON):
 * {
 *   "topic": "my-topic",
 *   "partition": 0,
 *   "offset": 123
 * }
 *
 * Features:
 * - Sticky partitioning: same key -> same partition
 * - Idempotent writes: idempotency_key ensures at-most-once duplication
 * - fsync: ACK only after WAL is durable
 * - Thread-safe: uses ConcurrentHashMap and atomic counters
 */
public class ProducerHandler implements HttpHandler {
    private final LogStore logStore;
    private final int defaultPartitions;
    
    // Sticky partitioning: key hash -> partition mapping
    private final Map<String, Integer> keyPartitionMap = new ConcurrentHashMap<>();
    
    // Partition counter for round-robin (when no key)
    private final AtomicInteger partitionCounter = new AtomicInteger(0);

    public ProducerHandler(LogStore logStore, int defaultPartitions) {
        this.logStore = logStore;
        this.defaultPartitions = defaultPartitions;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equals("POST")) {
            sendError(exchange, 405, "Method not allowed");
            return;
        }

        try {
            handleProduce(exchange);
        } catch (Exception e) {
            sendError(exchange, 500, "Internal error: " + e.getMessage());
        }
    }

    /**
     * Handle produce request.
     * Implements:
     * 1. Sticky partitioning
     * 2. Idempotent write deduplication
     * 3. fsync for durability
     */
    private void handleProduce(HttpExchange exchange) throws IOException {
        // Parse request body
        byte[] body = readRequest(exchange);
        Map<String, String> request = parseJson(body);

        String topic = request.get("topic");
        String keyB64 = request.get("key");
        String valueB64 = request.get("value");
        String idempotencyKey = request.get("idempotency_key");

        if (topic == null || valueB64 == null) {
            sendError(exchange, 400, "Missing: topic or value");
            return;
        }

        // Decode payload
        byte[] key = keyB64 != null ? Base64.getDecoder().decode(keyB64) : null;
        byte[] value = Base64.getDecoder().decode(valueB64);

        // Determine partition using sticky partitioning
        int partition = selectPartition(topic, key);

        try {
            // Get or create log for this partition
            var log = logStore.getOrCreateLog(topic, partition);

            // Create message
            Message message = new Message(
                topic, partition, 0, System.currentTimeMillis(),
                key, value, idempotencyKey
            );

            // Append to log
            // This handles idempotent deduplication internally
            long offset = log.append(message);

            if (offset < 0) {
                // Should not happen with current implementation
                sendError(exchange, 500, "Failed to append message");
                return;
            }

            // Return success
            String response = String.format(
                "{\"topic\":\"%s\",\"partition\":%d,\"offset\":%d}",
                topic, partition, offset
            );
            sendResponse(exchange, 200, response);

        } catch (IOException e) {
            sendError(exchange, 500, "Failed to write message: " + e.getMessage());
        }
    }

    /**
     * Select partition for a message using sticky partitioning.
     *
     * Strategy:
     * - If key is provided: hash(key) mod partitions -> ensures same key always goes to same partition
     * - If no key: round-robin to distribute load
     *
     * This is thread-safe because ConcurrentHashMap handles concurrent updates safely.
     */
    private int selectPartition(String topic, byte[] key) {
        if (key != null) {
            // Sticky partitioning: same key always maps to same partition
            String keyStr = Arrays.toString(key);
            return keyPartitionMap.computeIfAbsent(keyStr, k -> {
                int hash = Arrays.hashCode(key);
                return Math.abs(hash) % defaultPartitions;
            });
        } else {
            // No key: round-robin distribution
            return Math.abs(partitionCounter.getAndIncrement()) % defaultPartitions;
        }
    }

    /**
     * Read HTTP request body.
     */
    private byte[] readRequest(HttpExchange exchange) throws IOException {
        try (InputStream is = exchange.getRequestBody()) {
            return is.readAllBytes();
        }
    }

    /**
     * Simple JSON parser (minimal - production would use a proper JSON library).
     */
    private Map<String, String> parseJson(byte[] data) {
        Map<String, String> result = new HashMap<>();
        String json = new String(data, StandardCharsets.UTF_8);

        // Remove braces
        json = json.replaceAll("[{}]", "");

        // Split by comma
        for (String pair : json.split(",")) {
            String[] parts = pair.split(":");
            if (parts.length == 2) {
                String key = parts[0].replaceAll("[\"\\s]", "");
                String value = parts[1].replaceAll("[\"\\s]", "");
                result.put(key, value);
            }
        }

        return result;
    }

    private void sendResponse(HttpExchange exchange, int code, String body) throws IOException {
        byte[] response = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(code, response.length);
        exchange.getResponseBody().write(response);
        exchange.close();
    }

    private void sendError(HttpExchange exchange, int code, String message) throws IOException {
        String error = "{\"error\":\"" + message + "\"}";
        byte[] response = error.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(code, response.length);
        exchange.getResponseBody().write(response);
        exchange.close();
    }
}

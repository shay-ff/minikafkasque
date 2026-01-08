package com.kafka.jr.consumer;

import com.kafka.jr.log.LogStore;
import com.kafka.jr.model.Message;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * HTTP handler for consumer operations.
 * Endpoints:
 * - POST /join?group=<group>&consumer=<id>&topic=<topic>
 * - POST /leave?group=<group>&consumer=<id>
 * - GET /consume?group=<group>&consumer=<id>&topic=<topic>&partition=<p>&max_messages=<n>
 * - POST /commit?group=<group>&topic=<topic>&partition=<p>&offset=<o>
 *
 * Thread-safe: delegates to thread-safe LogStore and ConsumerGroupCoordinator.
 */
public class ConsumerHandler implements HttpHandler {
    private final LogStore logStore;
    private final ConsumerGroupCoordinator coordinator;

    public ConsumerHandler(LogStore logStore, ConsumerGroupCoordinator coordinator) {
        this.logStore = logStore;
        this.coordinator = coordinator;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        String path = exchange.getRequestURI().getPath();
        String query = exchange.getRequestURI().getQuery();

        try {
            if (path.equals("/join") && method.equals("POST")) {
                handleJoin(exchange, query);
            } else if (path.equals("/leave") && method.equals("POST")) {
                handleLeave(exchange, query);
            } else if (path.equals("/consume") && method.equals("GET")) {
                handleConsume(exchange, query);
            } else if (path.equals("/commit") && method.equals("POST")) {
                handleCommit(exchange, query);
            } else {
                sendError(exchange, 404, "Not found");
            }
        } catch (Exception e) {
            sendError(exchange, 500, "Error: " + e.getMessage());
        }
    }

    /**
     * Join a consumer to a group.
     */
    private void handleJoin(HttpExchange exchange, String query) throws IOException {
        Map<String, String> params = parseQuery(query);
        String group = params.get("group");
        String consumerId = params.get("consumer");
        String topic = params.get("topic");

        if (group == null || consumerId == null || topic == null) {
            sendError(exchange, 400, "Missing: group, consumer, or topic");
            return;
        }

        // Register topic for this group
        Set<Integer> partitions = getTopicPartitions(topic);
        coordinator.registerTopicPartitions(group, topic, partitions);

        // Join the group
        Map<String, List<Integer>> assignment = coordinator.joinGroup(group, consumerId);

        StringBuilder response = new StringBuilder();
        response.append("{\"status\":\"joined\",\"assignments\":{");
        boolean first = true;
        for (Map.Entry<String, List<Integer>> entry : assignment.entrySet()) {
            if (!first) response.append(",");
            response.append("\"").append(entry.getKey()).append("\":[");
            boolean firstPartition = true;
            for (int p : entry.getValue()) {
                if (!firstPartition) response.append(",");
                response.append(p);
                firstPartition = false;
            }
            response.append("]");
            first = false;
        }
        response.append("}}");

        sendResponse(exchange, 200, response.toString());
    }

    /**
     * Leave a consumer from a group.
     */
    private void handleLeave(HttpExchange exchange, String query) throws IOException {
        Map<String, String> params = parseQuery(query);
        String group = params.get("group");
        String consumerId = params.get("consumer");

        if (group == null || consumerId == null) {
            sendError(exchange, 400, "Missing: group or consumer");
            return;
        }

        coordinator.leaveGroup(group, consumerId);
        sendResponse(exchange, 200, "{\"status\":\"left\"}");
    }

    /**
     * Consume messages from a partition.
     */
    private void handleConsume(HttpExchange exchange, String query) throws IOException {
        Map<String, String> params = parseQuery(query);
        String group = params.get("group");
        String consumerId = params.get("consumer");
        String topic = params.get("topic");
        String partitionStr = params.get("partition");
        String maxMessagesStr = params.getOrDefault("max_messages", "100");

        if (group == null || consumerId == null || topic == null || partitionStr == null) {
            sendError(exchange, 400, "Missing: group, consumer, topic, or partition");
            return;
        }

        int partition = Integer.parseInt(partitionStr);
        int maxMessages = Integer.parseInt(maxMessagesStr);

        // Get last committed offset
        long startOffset = coordinator.getCommittedOffset(group, topic, partition);

        // Read messages
        var log = logStore.getLog(topic, partition);
        if (log == null) {
            sendError(exchange, 404, "Partition not found");
            return;
        }

        List<Message> messages = log.read(startOffset, maxMessages);

        // Build JSON response
        StringBuilder response = new StringBuilder();
        response.append("{\"messages\":[");
        for (int i = 0; i < messages.size(); i++) {
            if (i > 0) response.append(",");
            Message msg = messages.get(i);
            response.append("{\"offset\":").append(msg.getOffset()).append(",");
            response.append("\"timestamp\":").append(msg.getTimestamp()).append(",");
            response.append("\"key\":\"").append(base64Encode(msg.getKey())).append("\",");
            response.append("\"value\":\"").append(base64Encode(msg.getValue())).append("\"}");
        }
        response.append("],\"next_offset\":").append(startOffset + messages.size()).append("}");

        sendResponse(exchange, 200, response.toString());
    }

    /**
     * Commit an offset.
     */
    private void handleCommit(HttpExchange exchange, String query) throws IOException {
        Map<String, String> params = parseQuery(query);
        String group = params.get("group");
        String topic = params.get("topic");
        String partitionStr = params.get("partition");
        String offsetStr = params.get("offset");

        if (group == null || topic == null || partitionStr == null || offsetStr == null) {
            sendError(exchange, 400, "Missing: group, topic, partition, or offset");
            return;
        }

        int partition = Integer.parseInt(partitionStr);
        long offset = Long.parseLong(offsetStr);

        coordinator.commitOffset(group, topic, partition, offset);
        sendResponse(exchange, 200, "{\"status\":\"committed\"}");
    }

    /**
     * Get all partitions for a topic.
     * In a real system, this would query metadata.
     */
    private Set<Integer> getTopicPartitions(String topic) {
        // For simplicity, assume 3 partitions per topic by default
        // In reality, this would be determined by admin API or metadata
        Set<Integer> partitions = new HashSet<>();
        for (int i = 0; i < 3; i++) {
            partitions.add(i);
        }
        return partitions;
    }

    private Map<String, String> parseQuery(String query) {
        Map<String, String> params = new HashMap<>();
        if (query != null) {
            for (String pair : query.split("&")) {
                String[] parts = pair.split("=");
                if (parts.length == 2) {
                    params.put(parts[0], parts[1]);
                }
            }
        }
        return params;
    }

    private String base64Encode(byte[] data) {
        if (data == null) return "";
        return Base64.getEncoder().encodeToString(data);
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

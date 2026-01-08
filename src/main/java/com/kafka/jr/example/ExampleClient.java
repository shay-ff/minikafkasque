package com.kafka.jr.example;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Example client demonstrating Kafka-JR usage.
 * Shows:
 * 1. Producing messages with idempotency keys
 * 2. Joining a consumer group
 * 3. Consuming messages
 * 4. Committing offsets
 */
public class ExampleClient {
    private static final String BASE_URL = "http://localhost:8080";
    private static final HttpClient client = HttpClient.newHttpClient();

    public static void main(String[] args) throws IOException, InterruptedException {
        String topic = "events";
        String consumerGroup = "processors";
        String consumerId = "processor-1";
        int partition = 0;

        System.out.println("=== Kafka-JR Example Client ===\n");

        // 1. Produce 3 messages
        System.out.println("1. Producing messages...");
        for (int i = 1; i <= 3; i++) {
            String value = String.format("event-%d", i);
            String idempotencyKey = String.format("msg-%d", i);
            produceMessage(topic, "user-123", value, idempotencyKey);
            System.out.println("   ✓ Produced: " + value);
        }

        System.out.println();

        // 2. Join consumer group
        System.out.println("2. Joining consumer group...");
        joinGroup(consumerGroup, consumerId, topic);
        System.out.println("   ✓ Joined group: " + consumerGroup);

        System.out.println();

        // 3. Consume messages
        System.out.println("3. Consuming messages...");
        long nextOffset = consumeMessages(consumerGroup, consumerId, topic, partition, 10);
        System.out.println("   ✓ Fetched messages");

        System.out.println();

        // 4. Commit offset
        System.out.println("4. Committing offset...");
        commitOffset(consumerGroup, topic, partition, nextOffset);
        System.out.println("   ✓ Committed offset: " + nextOffset);

        System.out.println();

        // 5. Test idempotency (re-produce same message)
        System.out.println("5. Testing idempotency (re-produce with same idempotency_key)...");
        String duplicateValue = "event-1";
        String duplicateKey = "msg-1";
        produceMessage(topic, "user-123", duplicateValue, duplicateKey);
        System.out.println("   ✓ Should return same offset as original");

        System.out.println();

        // 6. Health check
        System.out.println("6. Health check...");
        healthCheck();
        System.out.println("   ✓ Broker is healthy");

        System.out.println("\n=== Example Complete ===");
    }

    private static void produceMessage(String topic, String key, String value, String idempotencyKey)
            throws IOException, InterruptedException {
        String keyB64 = Base64.getEncoder().encodeToString(key.getBytes(StandardCharsets.UTF_8));
        String valueB64 = Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));

        String json = String.format(
            "{\"topic\":\"%s\",\"key\":\"%s\",\"value\":\"%s\",\"idempotency_key\":\"%s\"}",
            topic, keyB64, valueB64, idempotencyKey
        );

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(BASE_URL + "/produce"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(json))
            .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == 200) {
            System.out.println("       " + response.body());
        } else {
            System.err.println("       Error: " + response.body());
        }
    }

    private static void joinGroup(String group, String consumerId, String topic)
            throws IOException, InterruptedException {
        String uri = String.format("%s/join?group=%s&consumer=%s&topic=%s",
            BASE_URL, group, consumerId, topic);

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(uri))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == 200) {
            System.out.println("       " + response.body());
        } else {
            System.err.println("       Error: " + response.body());
        }
    }

    private static long consumeMessages(String group, String consumerId, String topic, int partition, int maxMessages)
            throws IOException, InterruptedException {
        String uri = String.format("%s/consume?group=%s&consumer=%s&topic=%s&partition=%d&max_messages=%d",
            BASE_URL, group, consumerId, topic, partition, maxMessages);

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(uri))
            .GET()
            .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        System.out.println("       " + response.body());

        // Extract next_offset from response
        String body = response.body();
        int idx = body.indexOf("\"next_offset\":");
        if (idx != -1) {
            int endIdx = body.indexOf("}", idx);
            String nextOffsetStr = body.substring(idx + 14, endIdx).trim();
            return Long.parseLong(nextOffsetStr);
        }
        return 0;
    }

    private static void commitOffset(String group, String topic, int partition, long offset)
            throws IOException, InterruptedException {
        String uri = String.format("%s/commit?group=%s&topic=%s&partition=%d&offset=%d",
            BASE_URL, group, topic, partition, offset);

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(uri))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == 200) {
            System.out.println("       " + response.body());
        } else {
            System.err.println("       Error: " + response.body());
        }
    }

    private static void healthCheck() throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(BASE_URL + "/health"))
            .GET()
            .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        System.out.println("       " + response.body());
    }
}

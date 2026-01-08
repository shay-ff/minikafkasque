package com.kafka.jr.broker;

import com.kafka.jr.consumer.ConsumerGroupCoordinator;
import com.kafka.jr.consumer.ConsumerHandler;
import com.kafka.jr.log.LogStore;
import com.kafka.jr.producer.ProducerHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Main Kafka-JR broker server.
 *
 * Architecture:
 * 1. HTTP Server (java.com.sun.net.httpserver) for API endpoints
 * 2. LogStore for managing commit logs per topic-partition
 * 3. ConsumerGroupCoordinator for managing consumer groups and offsets
 * 4. ProducerHandler for /produce endpoint
 * 5. ConsumerHandler for /consume, /join, /leave endpoints
 *
 * All components are thread-safe for concurrent producers and consumers.
 *
 * On startup:
 * - Loads all segment files from disk
 * - Recovers offsets and idempotency state
 * - Starts HTTP server
 *
 * Failure handling:
 * - All writes are fsync'd for durability
 * - Crash recovery replays WAL on startup
 * - Idempotency keys prevent duplicate processing
 */
public class KafkaJRBroker {
    private final int port;
    private final String dataDir;
    private HttpServer httpServer;
    private LogStore logStore;
    private ConsumerGroupCoordinator coordinator;

    public KafkaJRBroker(int port, String dataDir) {
        this.port = port;
        this.dataDir = dataDir;
    }

    /**
     * Start the broker.
     * - Initializes log store (crash recovery happens here)
     * - Initializes consumer coordinator
     * - Starts HTTP server with thread pool for concurrent requests
     */
    public void start() throws IOException {
        System.out.println("=== Kafka-JR Broker Startup ===");
        System.out.println("Port: " + port);
        System.out.println("Data Dir: " + dataDir);

        // Initialize log store (loads and replays segments)
        logStore = new LogStore(Paths.get(dataDir));
        System.out.println("✓ LogStore initialized");

        // Initialize consumer coordinator
        coordinator = new ConsumerGroupCoordinator(Paths.get(dataDir));
        System.out.println("✓ ConsumerGroupCoordinator initialized");

        // Create HTTP server
        httpServer = HttpServer.create(new InetSocketAddress("0.0.0.0", port), 0);
        
        // Use thread pool for concurrent request handling
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors() * 2
        );
        httpServer.setExecutor(executor);

        // Register handlers
        httpServer.createContext("/produce", new ProducerHandler(logStore, 3));
        httpServer.createContext("/consume", new ConsumerHandler(logStore, coordinator));
        httpServer.createContext("/join", new ConsumerHandler(logStore, coordinator));
        httpServer.createContext("/leave", new ConsumerHandler(logStore, coordinator));
        httpServer.createContext("/commit", new ConsumerHandler(logStore, coordinator));

        // Health check
        httpServer.createContext("/health", exchange -> {
            String response = "{\"status\":\"healthy\"}";
            byte[] bytes = response.getBytes();
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });

        httpServer.start();
        System.out.println("✓ HTTP Server started on port " + port);
        System.out.println("\nBroker ready for requests:");
        System.out.println("  POST   /produce");
        System.out.println("  GET    /consume");
        System.out.println("  POST   /join");
        System.out.println("  POST   /leave");
        System.out.println("  POST   /commit");
        System.out.println("  GET    /health");
    }

    /**
     * Stop the broker gracefully.
     */
    public void stop() {
        System.out.println("\n=== Kafka-JR Broker Shutdown ===");
        
        if (httpServer != null) {
            httpServer.stop(5);
            System.out.println("✓ HTTP Server stopped");
        }

        if (coordinator != null) {
            try {
                coordinator.close();
                System.out.println("✓ ConsumerGroupCoordinator closed");
            } catch (IOException e) {
                System.err.println("Error closing coordinator: " + e.getMessage());
            }
        }

        if (logStore != null) {
            try {
                logStore.close();
                System.out.println("✓ LogStore closed");
            } catch (IOException e) {
                System.err.println("Error closing log store: " + e.getMessage());
            }
        }

        System.out.println("Broker shutdown complete");
    }

    public static void main(String[] args) throws IOException {
        int port = 8080;
        String dataDir = "data/kafka-jr";

        // Parse command-line arguments
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--port") && i + 1 < args.length) {
                port = Integer.parseInt(args[i + 1]);
            } else if (args[i].equals("--data-dir") && i + 1 < args.length) {
                dataDir = args[i + 1];
            }
        }

        KafkaJRBroker broker = new KafkaJRBroker(port, dataDir);

        // Shutdown hook for graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(broker::stop));

        try {
            broker.start();
            System.out.println("\nPress Ctrl+C to stop the broker");
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

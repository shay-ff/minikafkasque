# Kafka-JR: Minimal Kafka-like Distributed Commit Log

A production-grade implementation of a distributed commit log in **vanilla Java** (no frameworks, no Kafka libs) designed to be architecturally correct and interview-defensible.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                     Kafka-JR Broker                         │
├─────────────────────────────────────────────────────────────┤
│  HTTP Server (java.com.sun.net.httpserver)                  │
│  ├─ /produce       (ProducerHandler)                        │
│  ├─ /consume       (ConsumerHandler)                        │
│  ├─ /join          (ConsumerHandler)                        │
│  ├─ /leave         (ConsumerHandler)                        │
│  └─ /commit        (ConsumerHandler)                        │
├─────────────────────────────────────────────────────────────┤
│  LogStore (Topic-Partition Log Management)                  │
│  ├─ CommitLog[topic-0]  → Segment Files                    │
│  ├─ CommitLog[topic-1]  → Segment Files                    │
│  └─ CommitLog[topic-2]  → Segment Files                    │
├─────────────────────────────────────────────────────────────┤
│  ConsumerGroupCoordinator                                   │
│  ├─ Consumer Group Membership                              │
│  ├─ Partition Assignment (Sticky Strategy)                 │
│  └─ Committed Offset Tracking                              │
├─────────────────────────────────────────────────────────────┤
│  Disk Storage                                               │
│  ├─ data/kafka-jr/topic-0/0/00000000000000000000.log       │
│  ├─ data/kafka-jr/topic-0/0/00000000000100000000.log       │
│  ├─ data/kafka-jr/consumer_offsets.dat                     │
│  └─ ...                                                     │
└─────────────────────────────────────────────────────────────┘
```

## Key Features

### 1. **Write-Ahead Log (WAL)**
- **Append-only segment files**: Messages are written to disk-backed log segments
- **Segment rotation**: When a segment exceeds `MAX_SEGMENT_SIZE` (100MB), a new segment is created
- **Size-prefixed messages**: Each message prefixed with 4-byte size for reliable framing
- **fsync durability**: Every write waits for `FileChannel.force(true)` before ACKing to producer
- **Crash recovery**: On startup, broker replays all segments to recover offsets and idempotency state

**Message Format on Disk:**
```
[4 bytes: message size]
[4 bytes: topic length][variable: topic bytes]
[4 bytes: partition]
[8 bytes: offset]
[8 bytes: timestamp]
[4 bytes: key length][variable: key bytes]
[4 bytes: value length][variable: value bytes]
[4 bytes: idempotency key length][variable: idempotency key bytes]
```

### 2. **Partitioning Strategy**

#### Sticky Partitioning
- **Same key → Same partition**: Hash-based mapping ensures ordering for related messages
- **No key → Round-robin**: Distributes load evenly across partitions
- **Thread-safe**: Uses `ConcurrentHashMap` for atomic key→partition assignment

```java
int selectPartition(String topic, byte[] key) {
    if (key != null) {
        // hash(key) % partitions → ensures same partition
        return keyPartitionMap.computeIfAbsent(key_string, k -> {
            int hash = Arrays.hashCode(key);
            return Math.abs(hash) % defaultPartitions;
        });
    } else {
        // round-robin for unkeyed messages
        return Math.abs(partitionCounter.getAndIncrement()) % defaultPartitions;
    }
}
```

### 3. **Idempotent Producer (At-Least-Once Semantics)**

- **Idempotency Key**: Producers supply a unique `idempotency_key` per message
- **Deduplication**: CommitLog tracks all idempotency keys in an in-memory `Map<String, Long>`
- **Duplicate detection**: If same idempotency_key is sent twice, return the original offset
- **Crash-safe**: Map is reconstructed from disk on startup by replaying WAL

```java
public long append(Message message) {
    // Check for duplicate
    if (message.getIdempotencyKey() != null) {
        Long existingOffset = idempotencyMap.get(message.getIdempotencyKey());
        if (existingOffset != null) {
            return existingOffset; // Deduplicate
        }
    }
    // ... write new message
}
```

### 4. **Consumer Groups & Offset Management**

#### Consumer Group Coordination
- **Group Membership**: Consumers join/leave groups with `joinGroup()` and `leaveGroup()`
- **Partition Assignment**: Sticky assignment minimizes partition movement during rebalancing
- **Offset Tracking**: Each `(consumerGroup, topic, partition)` has a committed offset
- **Persistence**: Offsets persisted to `consumer_offsets.dat` via `ObjectOutputStream`

#### Sticky Partition Assignment
```
Group: "web-team", Consumers: [alice, bob]
Topic: "events", Partitions: [0, 1, 2]

Initial assignment:
  alice → [0, 2]
  bob   → [1]

If charlie joins:
  alice → [0]       (sticky: tries to keep original partitions)
  bob   → [1]
  charlie → [2]
```

### 5. **Consumer Offset Tracking**

- **Committed offsets**: Stored in-memory in `ConsumerGroupCoordinator`
- **Fetch on startup**: Replayed from `consumer_offsets.dat` on broker restart
- **Per-group granularity**: Different consumer groups track independent offsets
- **Prevents offset corruption**: Uses `ConsumerOffsetKey` (group, topic, partition) as key

```java
public class ConsumerOffsetKey {
    String consumerGroup;
    String topic;
    int partition;
    
    // Ensures unique offset tracking per group per partition
}
```

### 6. **Concurrency & Thread-Safety**

**CommitLog (per partition):**
- `ReadWriteLock`: Read locks for `read()` operations, write locks for `append()`
- Allows concurrent consumers to read while writes are exclusive

**LogSegment:**
- Synchronized `append()` and `fsync()`: Only one writer at a time
- Synchronized `readAt()` and `readFrom()`: Multiple concurrent readers

**LogStore:**
- `ConcurrentHashMap<TopicPartition, CommitLog>`: Lock-free lookups
- Double-checked locking for partition creation

**ProducerHandler:**
- `AtomicInteger` for round-robin partition selection
- `ConcurrentHashMap` for sticky partition mapping
- Stateless: no synchronization needed (idempotency handled by CommitLog)

**ConsumerGroupCoordinator:**
- `ReentrantReadWriteLock`: For group membership and partition assignment
- `ConcurrentHashMap` for offset storage
- Immutable `ConsumerOffsetKey` for thread-safe map lookups

### 7. **Crash Recovery**

**Scenario 1: Broker crash after write but before ACK**
```
1. Producer sends message
2. Broker appends to WAL and fsyncs
3. Broker crashes before sending ACK
4. On restart:
   - Replay all WAL segments
   - Recover offset counter and idempotency map
   - Clients retry produce() → idempotency key deduplicates
5. Message is not lost, but producer must retry
```

**Scenario 2: Partial write at end of file**
```
1. Broker starts appending message N
2. Writes size prefix [4 bytes]
3. Crashes while writing message data
4. On recovery:
   - CommitLog.recover() reads segment
   - Encounters incomplete message
   - Stops recovery and discards incomplete message
   - No corruption of previous messages
5. Message N is lost, but consistency is maintained
```

**Scenario 3: Consumer group metadata loss**
```
1. Broker crash while consumer offsets in memory
2. On restart:
   - Load consumer_offsets.dat
   - All committed offsets are recovered
   - Next consume() call resumes from last committed offset
3. No data loss for committed offsets
```

## API Endpoints

### Producer API

**POST /produce**

Request body (JSON):
```json
{
  "topic": "user-events",
  "key": "YWxpY2U=",                    // base64-encoded key (optional)
  "value": "eyJpZCI6IDEyMzR9",          // base64-encoded value
  "idempotency_key": "req-12345-67890"  // unique ID for deduplication
}
```

Response (200 OK):
```json
{
  "topic": "user-events",
  "partition": 2,
  "offset": 1025
}
```

**Features:**
- Sticky partitioning: same key → same partition
- At-least-once semantics: idempotency_key ensures no duplicates
- fsync before ACK: durable write guarantee

### Consumer API

**POST /join**

Query parameters:
- `group`: consumer group name
- `consumer`: consumer ID
- `topic`: topic to subscribe to

Response (200 OK):
```json
{
  "status": "joined",
  "assignments": {
    "user-events": [0, 2]
  }
}
```

**Features:**
- Returns partition assignment for this consumer
- Triggers rebalancing if this is a new consumer

---

**POST /leave**

Query parameters:
- `group`: consumer group name
- `consumer`: consumer ID

Response (200 OK):
```json
{
  "status": "left"
}
```

---

**GET /consume**

Query parameters:
- `group`: consumer group name
- `consumer`: consumer ID (for logging)
- `topic`: topic to consume from
- `partition`: partition number
- `max_messages`: max messages to return (default: 100)

Response (200 OK):
```json
{
  "messages": [
    {
      "offset": 1020,
      "timestamp": 1704758400000,
      "key": "YWxpY2U=",
      "value": "eyJpZCI6IDEyMzR9"
    },
    {
      "offset": 1021,
      "timestamp": 1704758401000,
      "key": "YWxpY2U=",
      "value": "eyJpZCI6IDEyMzV9"
    }
  ],
  "next_offset": 1022
}
```

**Features:**
- Fetches from last committed offset
- Returns messages in offset order
- Client must call `/commit` to advance offset

---

**POST /commit**

Query parameters:
- `group`: consumer group name
- `topic`: topic
- `partition`: partition
- `offset`: offset to commit

Response (200 OK):
```json
{
  "status": "committed"
}
```

## Delivery Guarantees

### At-Least-Once Semantics
- **Producer**: Retries are idempotent (same `idempotency_key` → same offset)
- **Broker**: fsync ensures durability before ACK
- **Consumer**: Offset is committed explicitly by application
- **Implication**: Messages may be processed twice if consumer crashes after processing but before committing offset

```
Producer side:
  1. Send(msg, idempotency_key=X)
  2. [ACK received] → Done
  3. [No ACK, timeout] → Retry with same idempotency_key=X
  4. Broker deduplicates on idempotency_key

Consumer side:
  1. Consume messages
  2. Process messages
  3. Commit offset
  4. [Crash between step 2 and 3] → Next consumer re-processes message
```

### Exactly-Once (Application-Level)
For exactly-once end-to-end guarantees, applications must:
1. Use idempotency keys on the producer side
2. Make consumer processing idempotent (e.g., upsert instead of insert)
3. Use atomic transactions to tie offset commits to processing

## Building & Running

### Build
```bash
cd /Users/shayan/dev/kafka-jr
javac -d target/classes -sourcepath src/main/java src/main/java/com/kafka/jr/**/*.java
```

Or using jar:
```bash
cd src/main/java
javac -d ../../../../target/classes com/kafka/jr/**/*.java
jar cf ../../../../kafka-jr.jar -C ../../../../target/classes .
```

### Run
```bash
java -cp kafka-jr.jar com.kafka.jr.broker.KafkaJRBroker --port 8080 --data-dir data/kafka-jr
```

### Health Check
```bash
curl http://localhost:8080/health
```

## Example: Producing Messages

```bash
# Message 1
curl -X POST http://localhost:8080/produce \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "orders",
    "key": "dXNlcjEyMw==",
    "value": "eyJvcmRlcl9pZCI6IDEwMDF9",
    "idempotency_key": "order-1001-req-1"
  }'

# Response: {"topic":"orders","partition":1,"offset":0}

# Duplicate (same idempotency_key) - returns same offset
curl -X POST http://localhost:8080/produce \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "orders",
    "key": "dXNlcjEyMw==",
    "value": "eyJvcmRlcl9pZCI6IDEwMDF9",
    "idempotency_key": "order-1001-req-1"
  }'

# Response: {"topic":"orders","partition":1,"offset":0}  (same offset, deduplicated)
```

## Example: Consuming Messages

```bash
# Join consumer group
curl -X POST http://localhost:8080/join \
  -G -d "group=order-processors&consumer=processor-1&topic=orders"

# Response: {"status":"joined","assignments":{"orders":[0,2]}}

# Consume from partition 0
curl -X GET http://localhost:8080/consume \
  -G -d "group=order-processors&consumer=processor-1&topic=orders&partition=0&max_messages=10"

# Response: {"messages":[...],"next_offset":1}

# Commit offset after processing
curl -X POST http://localhost:8080/commit \
  -G -d "group=order-processors&topic=orders&partition=0&offset=1"

# Response: {"status":"committed"}
```

## Code Structure

```
src/main/java/com/kafka/jr/
├── broker/
│   └── KafkaJRBroker.java              # Main entry point, HTTP server setup
├── log/
│   ├── CommitLog.java                  # Per-partition log (WAL mgmt, crash recovery)
│   ├── LogSegment.java                 # Individual segment file (append, read, fsync)
│   └── LogStore.java                   # Central log repository
├── producer/
│   └── ProducerHandler.java            # HTTP handler for /produce (partitioning, idempotency)
├── consumer/
│   ├── ConsumerHandler.java            # HTTP handlers for /consume, /join, /leave, /commit
│   └── ConsumerGroupCoordinator.java   # Group membership, offset tracking, rebalancing
├── model/
│   ├── Message.java                    # Immutable message record (serialize/deserialize)
│   ├── TopicPartition.java             # Immutable topic-partition pair
│   └── ConsumerOffsetKey.java          # Immutable offset key (group, topic, partition)
└── util/
    └── FileUtils.java                  # File utilities

data/kafka-jr/
├── topic-name/
│   ├── 0/
│   │   ├── 00000000000000000000.log    # Segment 0-99
│   │   └── 00000000000000000100.log    # Segment 100-199
│   ├── 1/
│   │   └── 00000000000000000000.log
│   └── 2/
│       └── 00000000000000000000.log
└── consumer_offsets.dat                # Persisted consumer offsets
```

## Design Decisions

### 1. Why Java NIO, Not Netty?
- **Requirement**: Vanilla Java, no frameworks
- **Trade-off**: NIO's `HttpServer` is simpler, less scalable, but sufficient for interviews
- **Production alternative**: Netty or Spring for millions of RPS

### 2. Why ReadWriteLock for CommitLog?
- **Rationale**: Multiple consumers can read concurrently; writes are exclusive
- **Alternative**: Fine-grained locking per segment (complexity vs. performance trade-off)

### 3. Why Sticky Partitioning?
- **Benefit**: Messages with same key stay in same partition → preserves ordering
- **Example**: User ID as key → all events for a user go to same partition
- **Implementation**: `ConcurrentHashMap<keyString, partition>` for O(1) lookups

### 4. Why fsync Before ACK?
- **Durability**: ACK only after `FileChannel.force(true)`
- **Trade-off**: Slower writes (1,000s msgs/sec vs. 100,000s), but crash-safe
- **Production alternative**: Batched commits (fsync every 100ms or 1MB)

### 5. Why Separate Segment Files?
- **Compaction**: Old segments can be archived/deleted
- **Recovery speed**: Crash recovery doesn't need to scan 100GB files
- **Concurrency**: Reader can read old segments while writer appends to new one

## Limitations vs. Real Kafka

| Feature | Kafka-JR | Real Kafka | Reason |
|---------|----------|-----------|--------|
| **Replication** | Single node | Raft, leader-follower | Kafka-JR is single broker |
| **Persistence** | fsync per message | Batched fsync | Kafka-JR prioritizes safety |
| **Compression** | None | Snappy, GZIP, LZ4 | Vanilla Java only |
| **Transactions** | None | Transactional API | Out of scope for this project |
| **Consumer lag tracking** | Manual | Consumer Metrics API | Not implemented |
| **SASL/TLS** | None | Full TLS | HTTP only, no auth |
| **Metadata broker** | Hardcoded 3 partitions | Dynamic metadata | Assumes 3 partitions/topic |
| **Throughput** | ~1MB/sec | ~1GB/sec+ | Single-threaded WAL writer |

## Interview Notes

### What to Highlight

1. **WAL + Crash Recovery**: "All writes are persisted to disk before ACK. On startup, we replay the WAL to recover offset counters and idempotency state. This ensures no data loss even if the broker crashes mid-write."

2. **Idempotent Writes**: "Producers supply an idempotency_key. We track all keys in a map (reconstructed on startup). Duplicate requests return the same offset, ensuring at-least-once semantics."

3. **Sticky Partitioning**: "Messages with the same key hash to the same partition, preserving message ordering. We use a ConcurrentHashMap for O(1) lookups."

4. **Consumer Groups**: "Consumers join groups, which triggers sticky partition assignment. Offsets are tracked per group per partition and persisted to disk."

5. **Thread Safety**: "We use ReadWriteLock for CommitLog (multiple readers, exclusive writes), ConcurrentHashMap for partition mapping, and immutable message objects. No global locks."

### How to Extend

- **Replication**: Add a leader-follower pattern with a separate replication topic
- **Transactions**: Use a transaction log and two-phase commit for atomic writes
- **Compression**: Add compression/decompression in Message.serialize()
- **Authentication**: Add HMAC signature validation in HTTP handlers
- **Monitoring**: Add offset lag metrics in ConsumerHandler

## Testing

### Quick Smoke Test

```bash
# Terminal 1: Start broker
java -cp target/classes com.kafka.jr.broker.KafkaJRBroker

# Terminal 2: Produce 5 messages
for i in {1..5}; do
  curl -X POST http://localhost:8080/produce \
    -H "Content-Type: application/json" \
    -d "{\"topic\":\"test\",\"value\":\"$(echo -n 'msg$i' | base64)\",\"idempotency_key\":\"msg-$i\"}"
done

# Terminal 2: Consume them back
curl -X POST http://localhost:8080/join \
  -G -d "group=test-group&consumer=test-consumer&topic=test"

curl -X GET http://localhost:8080/consume \
  -G -d "group=test-group&consumer=test-consumer&topic=test&partition=0&max_messages=10"

# Terminal 2: Restart broker (kill Terminal 1, restart)
# Then consume again - should get same messages (recovery worked!)
```

---

## Author Notes

This implementation prioritizes **architectural clarity** and **interview defensibility** over performance:
- **Clear separation of concerns**: LogStore, CommitLog, Handlers are independent
- **Production-grade safety**: fsync, crash recovery, deduplication
- **Testable design**: Each component can be tested independently
- **Minimal dependencies**: Vanilla Java only

For production use, consider Kafka or RedPanda. For learning distributed systems, this is a great reference implementation.
# minikafkasque

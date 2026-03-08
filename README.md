# Kafka Ordering Deep Dive: From Basics to Production Patterns

A progressive, hands-on demo that teaches you how Kafka handles message ordering — and how to get both **speed** and **order** in production systems.

## The Problem

You're building an e-commerce system. Each order goes through a lifecycle:

```
CREATED → PAID → SHIPPED → DELIVERED
```

Processing these events out of order is catastrophic — you can't ship before payment, and you can't deliver before shipping. How does Kafka help you maintain this order at scale?

This demo walks you through three levels, each building on the last.

## The Three Levels

| Level | Partitions | Threads | Ordering | Throughput | Key Concept |
|-------|-----------|---------|----------|------------|-------------|
| **1** | 1 | 1 | Total | Low | Single partition = total order |
| **2** | 4 | 4 | Broken | High | More partitions = more speed, no order |
| **3** | 4 | 4 | Per-key | High | Partition keys = speed + order |

## Prerequisites

- **Docker** and **Docker Compose** (to run Kafka)
- **Java 17+** (to compile and run the demos)
- **Maven 3.8+** (to build)

## Quick Start

### 1. Start Kafka

```bash
docker compose up -d
```

Wait ~10 seconds for Kafka to be ready.

### 2. Create Topics

```bash
./setup_topics.sh
```

### 3. Build the Project

```bash
mvn clean compile
```

### 4. Run the Demos

Run each level's producer first, then consumer in a **separate terminal**.

---

## Level 1: Single Partition — Guaranteed Order

**Concept:** With one partition, Kafka is an append-only log. Messages come out in the exact order they go in.

```bash
# Terminal 1 — produce messages
mvn exec:java -Dexec.mainClass="com.demo.kafka.level1.Producer"

# Terminal 2 — consume and verify order
mvn exec:java -Dexec.mainClass="com.demo.kafka.level1.Consumer"
```

**What you'll see:**
```
SENT  Order ORD-001 → CREATED    | partition=0 offset=0
SENT  Order ORD-001 → PAID       | partition=0 offset=1
SENT  Order ORD-001 → SHIPPED    | partition=0 offset=2
SENT  Order ORD-001 → DELIVERED  | partition=0 offset=3
```

Every message hits partition 0. The consumer reads them sequentially. **Perfect order.**

**The tradeoff:** Only one consumer can read from one partition, so throughput is limited. As your system scales, this single partition becomes a bottleneck.

```
                ┌──────────────┐
Producer ──────►│ Partition 0  │──────► Consumer
                └──────────────┘
                 (ordered, slow)
```

---

## Level 2: Multi-Partition + Threads — Speed, No Order

**Concept:** We add 4 partitions and 4 producer threads. Throughput goes way up, but without a partition key, Kafka round-robins messages across partitions — and order breaks.

```bash
# Terminal 1
mvn exec:java -Dexec.mainClass="com.demo.kafka.level2.Producer"

# Terminal 2
mvn exec:java -Dexec.mainClass="com.demo.kafka.level2.Consumer"
```

**What you'll see:**
```
RECV  Order ORD-003 → SHIPPED      ✗   ← before PAID!
RECV  Order ORD-003 → CREATED      ✗
RECV  Order ORD-003 → PAID         ✗
RECV  Order ORD-003 → DELIVERED    ✓
```

The consumer's ordering analysis will show broken orders:
```
✗ ORD-003: SHIPPED → CREATED → PAID → DELIVERED
✗ ORD-007: PAID → CREATED → DELIVERED → SHIPPED
```

**Why?** Without a key, messages for the same order scatter across partitions. Different threads consume different partitions, and there's no cross-partition ordering guarantee.

```
                ┌──────────────┐
           ┌───►│ Partition 0  │───► Thread 0
           │    ├──────────────┤
Producer ──┼───►│ Partition 1  │───► Thread 1    (messages for ORD-001
(no key)   │    ├──────────────┤     land on ALL
           │───►│ Partition 2  │───► Thread 2     partitions randomly)
           │    ├──────────────┤
           └───►│ Partition 3  │───► Thread 3
                └──────────────┘
                 (fast, unordered)
```

---

## Level 3: Consistent Hashing — Speed AND Order

**Concept:** We use the `order_id` as the **partition key**. Kafka hashes the key to deterministically route all messages for the same order to the same partition. Order is preserved per key, and we still get parallel throughput across keys.

```bash
# Terminal 1
mvn exec:java -Dexec.mainClass="com.demo.kafka.level3.Producer"

# Terminal 2
mvn exec:java -Dexec.mainClass="com.demo.kafka.level3.Consumer"
```

**What you'll see:**
```
SENT  [Thread-0] Order ORD-001 → CREATED    | key=ORD-001 partition=2 offset=0
SENT  [Thread-0] Order ORD-001 → PAID       | key=ORD-001 partition=2 offset=1
SENT  [Thread-0] Order ORD-001 → SHIPPED    | key=ORD-001 partition=2 offset=2
SENT  [Thread-0] Order ORD-001 → DELIVERED  | key=ORD-001 partition=2 offset=3
```

Notice: same key → same partition, every time. The consumer confirms:

```
✓ ORD-001: CREATED → PAID → SHIPPED → DELIVERED  (partition 2)
✓ ORD-002: CREATED → PAID → SHIPPED → DELIVERED  (partition 0)
✓ ORD-003: CREATED → PAID → SHIPPED → DELIVERED  (partition 1)

RESULT: All 20 orders arrived in perfect order!
```

**How it works:**

```
                ┌──────────────┐
           ┌───►│ Partition 0  │───► Thread 0  (ORD-002, ORD-006, ORD-011)
           │    ├──────────────┤
Producer ──┼───►│ Partition 1  │───► Thread 1  (ORD-003, ORD-008, ORD-015)
(key=      │    ├──────────────┤
 order_id) ├───►│ Partition 2  │───► Thread 2  (ORD-001, ORD-005, ORD-012)
           │    ├──────────────┤
           └───►│ Partition 3  │───► Thread 3  (ORD-004, ORD-009, ORD-016)
                └──────────────┘
                 (fast, ordered per key)

hash("ORD-001") % 4 = 2  ← always partition 2
hash("ORD-002") % 4 = 0  ← always partition 0
```

---

## Key Takeaways

1. **Kafka guarantees ordering within a partition**, not across partitions.

2. **More partitions = more throughput**, because multiple consumers can work in parallel. But without a key, messages scatter randomly.

3. **Partition keys give you the best of both worlds.** Kafka hashes the key to pick a partition deterministically. All messages with the same key go to the same partition → they stay ordered.

4. **Choose your partition key carefully.** It should match your "unit of ordering":
   - Order events → key on `order_id`
   - User activity → key on `user_id`
   - IoT telemetry → key on `device_id`
   - Financial txns → key on `account_id`

5. **Watch for hot partitions.** If one key has way more traffic than others, its partition becomes a bottleneck. Good key design distributes load evenly.

## Cleanup

```bash
docker compose down -v
```

## Project Structure

```
kafka-demos/
├── docker-compose.yml                         # Kafka + Zookeeper
├── setup_topics.sh                            # Creates all demo topics
├── pom.xml                                    # Maven build config
└── src/main/java/com/demo/kafka/
    ├── level1/                                # Single partition demo
    │   ├── Producer.java
    │   └── Consumer.java
    ├── level2/                                # Multi-partition + threads
    │   ├── Producer.java
    │   └── Consumer.java
    └── level3/                                # Consistent hashing
        ├── Producer.java
        └── Consumer.java
```

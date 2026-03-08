package com.demo.kafka.level3;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Level 3 — Consistent Hashing Consumer (Best of Both Worlds)
 * =============================================================
 * Consumes from a 4-partition topic with multiple threads.
 *
 * Key insight: Because the producer used order_id as the partition key,
 * all events for the same order land on the same partition. Each consumer
 * thread processes one partition, so per-order ordering is guaranteed
 * while still getting parallel throughput across orders.
 *
 *     Partition 0: ORD-002, ORD-007, ORD-013  (all in order)
 *     Partition 1: ORD-001, ORD-005, ORD-018  (all in order)
 *     Partition 2: ORD-003, ORD-010, ORD-014  (all in order)
 *     Partition 3: ORD-004, ORD-006, ORD-019  (all in order)
 *
 * Run: mvn exec:java -Dexec.mainClass="com.demo.kafka.level3.Consumer"
 */
public class Consumer {

    private static final String TOPIC = "orders-level3";
    private static final String BROKER = "localhost:9092";
    private static final int NUM_THREADS = 4;

    // ANSI colors
    private static final String CYAN = "\u001B[96m";
    private static final String GREEN = "\u001B[92m";
    private static final String RED = "\u001B[91m";
    private static final String YELLOW = "\u001B[93m";
    private static final String BOLD = "\u001B[1m";
    private static final String RESET = "\u001B[0m";

    private static final List<Map<String, Object>> allMessages = new CopyOnWriteArrayList<>();

    public static void main(String[] args) throws Exception {
        System.out.println("=".repeat(70));
        System.out.println("  LEVEL 3: Consistent Hashing Consumer");
        System.out.println("  Consuming with multiple threads from 4 partitions");
        System.out.println("=".repeat(70));
        System.out.println();

        List<Thread> threads = new ArrayList<>();
        for (int t = 0; t < NUM_THREADS; t++) {
            int threadId = t;
            Thread thread = new Thread(() -> consumePartition(threadId));
            threads.add(thread);
        }

        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();

        // Analyze results
        System.out.println();
        System.out.println("=".repeat(70));
        System.out.printf("  %sORDERING ANALYSIS%s%n", BOLD, RESET);
        System.out.println("-".repeat(70));

        analyzeOrdering();
        analyzePartitionDistribution();
        printSummary();
    }

    private static void consumePartition(int threadId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "level3-consumer-group-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        ObjectMapper mapper = new ObjectMapper();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(TOPIC));

            int emptyPolls = 0;
            while (emptyPolls < 5) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));

                if (records.isEmpty()) {
                    emptyPolls++;
                    continue;
                }
                emptyPolls = 0;

                for (ConsumerRecord<String, String> record : records) {
                    JsonNode data = mapper.readTree(record.value());

                    Map<String, Object> entry = new LinkedHashMap<>();
                    entry.put("order_id", data.get("order_id").asText());
                    entry.put("status", data.get("status").asText());
                    entry.put("sequence", data.get("sequence").asInt());
                    entry.put("partition", record.partition());
                    entry.put("offset", record.offset());
                    entry.put("key", record.key());
                    allMessages.add(entry);

                    synchronized (System.out) {
                        System.out.printf("%sRECV%s  [Thread-%d] Order %s → %-10s | key=%s partition=%d offset=%d%n",
                                CYAN, RESET, threadId,
                                data.get("order_id").asText(),
                                data.get("status").asText(),
                                record.key(),
                                record.partition(), record.offset());
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void analyzeOrdering() {
        Map<String, List<Map<String, Object>>> orderEvents = new TreeMap<>();
        for (Map<String, Object> entry : allMessages) {
            String oid = (String) entry.get("order_id");
            orderEvents.computeIfAbsent(oid, k -> new ArrayList<>()).add(entry);
        }

        List<String> expectedFlow = List.of("CREATED", "PAID", "SHIPPED", "DELIVERED");
        int orderedCount = 0;
        int brokenCount = 0;

        for (Map.Entry<String, List<Map<String, Object>>> e : orderEvents.entrySet()) {
            List<String> actualFlow = e.getValue().stream()
                    .map(m -> (String) m.get("status"))
                    .toList();
            Set<Integer> partitions = new TreeSet<>();
            e.getValue().forEach(m -> partitions.add((Integer) m.get("partition")));

            if (actualFlow.equals(expectedFlow)) {
                orderedCount++;
                System.out.printf("  %s✓%s %s: %s  (partition %s)%n",
                        GREEN, RESET, e.getKey(),
                        String.join(" → ", actualFlow),
                        partitions.iterator().next());
            } else {
                brokenCount++;
                System.out.printf("  %s✗%s %s: %s  (partitions: %s)%n",
                        RED, RESET, e.getKey(),
                        String.join(" → ", actualFlow),
                        partitions);
            }
        }

        System.out.println();
        if (brokenCount == 0) {
            System.out.printf("  %s%sRESULT: All %d orders arrived in perfect order!%s%n",
                    GREEN, BOLD, orderedCount, RESET);
            System.out.println();
            System.out.println("  How? Consistent hashing ensures:");
            System.out.println("    1. Same key → same partition (deterministic)");
            System.out.println("    2. Within a partition → messages are ordered (Kafka guarantee)");
            System.out.println("    3. Each partition is consumed by one thread (consumer group)");
            System.out.println();
            System.out.println("  We get parallel throughput (4 partitions processed simultaneously)");
            System.out.println("  AND per-key ordering (all events for an order are sequential).");
        } else {
            System.out.printf("  %sRESULT: %d orders arrived out of order!%s%n", RED, brokenCount, RESET);
            System.out.println("  This shouldn't happen with proper key-based partitioning.");
        }
    }

    private static void analyzePartitionDistribution() {
        Map<Integer, Set<String>> partitionOrders = new TreeMap<>();
        for (Map<String, Object> entry : allMessages) {
            int p = (Integer) entry.get("partition");
            String oid = (String) entry.get("order_id");
            partitionOrders.computeIfAbsent(p, k -> new TreeSet<>()).add(oid);
        }

        System.out.printf("%n  %sPartition Distribution:%s%n", CYAN, RESET);
        for (Map.Entry<Integer, Set<String>> e : partitionOrders.entrySet()) {
            System.out.printf("    Partition %d: %s (%d orders)%n",
                    e.getKey(),
                    String.join(", ", e.getValue()),
                    e.getValue().size());
        }
    }

    private static void printSummary() {
        System.out.println();
        System.out.println("=".repeat(70));
        System.out.printf("  %sSUMMARY: The Three Levels%s%n", BOLD, RESET);
        System.out.println("-".repeat(70));
        System.out.printf("  Level 1: 1 partition  → %sordered%s, slow%n", GREEN, RESET);
        System.out.printf("  Level 2: 4 partitions → fast, %sunordered%s%n", RED, RESET);
        System.out.printf("  Level 3: 4 partitions → fast, %sordered per key%s%n", GREEN, RESET);
        System.out.println();
        System.out.println("  The secret sauce: partition keys + consistent hashing.");
        System.out.println("  Pick the right key for your use case and you get the");
        System.out.println("  best of both worlds.");
        System.out.println("=".repeat(70));
        System.out.println();
    }
}

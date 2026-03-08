package com.demo.kafka.level3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Level 3 — Consistent Hashing Producer (Best of Both Worlds)
 * =============================================================
 * Demonstrates how partition KEYS solve the ordering vs speed tradeoff.
 *
 * Same scenario: e-commerce order lifecycle events.
 * Same setup: 4 partitions, 4 producer threads.
 *
 * The fix: We set the message KEY to the order_id. Kafka hashes the key
 * to determine the partition. Same key → same partition → same order.
 *
 *     hash("ORD-001") % 4 = partition 2  (always!)
 *     hash("ORD-002") % 4 = partition 0  (always!)
 *
 * This is consistent hashing — the mapping from key to partition is
 * deterministic, so all events for one order land on the same partition.
 *
 * Run: mvn exec:java -Dexec.mainClass="com.demo.kafka.level3.Producer"
 */
public class Producer {

    private static final String TOPIC = "orders-level3";
    private static final String BROKER = "localhost:9092";
    private static final int NUM_THREADS = 4;
    private static final String[] ORDER_STAGES = {"CREATED", "PAID", "SHIPPED", "DELIVERED"};

    // ANSI colors
    private static final String GREEN = "\u001B[92m";
    private static final String CYAN = "\u001B[96m";
    private static final String RESET = "\u001B[0m";

    private static final Map<String, Integer> partitionMap = new ConcurrentHashMap<>();

    public static void main(String[] args) throws Exception {
        System.out.println("=".repeat(70));
        System.out.println("  LEVEL 3: Consistent Hashing Producer");
        System.out.println("  Topic has 4 partitions + 4 threads + PARTITION KEYS");
        System.out.printf("  %sUsing order_id as key → same order always hits same partition%s%n", CYAN, RESET);
        System.out.println("=".repeat(70));
        System.out.println();

        int ordersPerThread = 5;
        List<Thread> threads = new ArrayList<>();

        long startTime = System.currentTimeMillis();

        for (int t = 0; t < NUM_THREADS; t++) {
            int threadId = t;
            int startOrder = t * ordersPerThread + 1;
            int endOrder = startOrder + ordersPerThread;

            Thread thread = new Thread(() -> {
                try {
                    produceOrders(threadId, startOrder, endOrder);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            threads.add(thread);
        }

        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();

        long elapsed = System.currentTimeMillis() - startTime;

        System.out.println();
        System.out.println("-".repeat(70));
        System.out.printf("  All messages sent in %dms using %d threads.%n", elapsed, NUM_THREADS);
        System.out.println();

        // Show the key → partition mapping
        System.out.printf("  %sKey → Partition Mapping (consistent hashing):%s%n%n", CYAN, RESET);
        partitionMap.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(e -> {
                    String bar = "█".repeat(e.getValue() + 1);
                    System.out.printf("    %s → partition %d  %s%n", e.getKey(), e.getValue(), bar);
                });

        System.out.println();
        System.out.println("  Notice: each order_id ALWAYS maps to the same partition.");
        System.out.println("  This is deterministic — run it again and the mapping is identical.");
        System.out.println();
        System.out.println("  Now run the Consumer to verify ordering is preserved!");
        System.out.println("-".repeat(70));
    }

    private static void produceOrders(int threadId, int startOrder, int endOrder) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        ObjectMapper mapper = new ObjectMapper();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int orderId = startOrder; orderId < endOrder; orderId++) {
                String orderKey = String.format("ORD-%03d", orderId);

                for (int seq = 0; seq < ORDER_STAGES.length; seq++) {
                    String stage = ORDER_STAGES[seq];

                    ObjectNode message = mapper.createObjectNode();
                    message.put("order_id", orderKey);
                    message.put("status", stage);
                    message.put("sequence", seq);
                    message.put("thread_id", threadId);
                    message.put("timestamp", System.currentTimeMillis());

                    // THE KEY DIFFERENCE: we pass key=orderKey
                    // Kafka hashes this key to pick the partition deterministically
                    ProducerRecord<String, String> record =
                            new ProducerRecord<>(TOPIC, orderKey, mapper.writeValueAsString(message));

                    RecordMetadata metadata = producer.send(record).get();
                    partitionMap.put(orderKey, metadata.partition());

                    synchronized (System.out) {
                        System.out.printf("%sSENT%s  [Thread-%d] Order %s → %-10s | key=%s partition=%d offset=%d%n",
                                GREEN, RESET, threadId, orderKey, stage,
                                orderKey, metadata.partition(), metadata.offset());
                    }

                    Thread.sleep(50);
                }
            }
        }
    }
}

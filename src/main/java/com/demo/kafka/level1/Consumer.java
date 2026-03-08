package com.demo.kafka.level1;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

/**
 * Level 1 — Single Partition Consumer
 * =====================================
 * Reads from a single-partition topic and verifies message ordering.
 *
 * Key insight: Every message arrives in the exact order it was produced.
 * This is Kafka's fundamental guarantee within a partition.
 *
 * Run: mvn exec:java -Dexec.mainClass="com.demo.kafka.level1.Consumer"
 */
public class Consumer {

    private static final String TOPIC = "orders-level1";
    private static final String BROKER = "localhost:9092";

    // ANSI colors
    private static final String CYAN = "\u001B[96m";
    private static final String GREEN = "\u001B[92m";
    private static final String RED = "\u001B[91m";
    private static final String YELLOW = "\u001B[93m";
    private static final String BOLD = "\u001B[1m";
    private static final String RESET = "\u001B[0m";

    private static final Map<String, Integer> EXPECTED_FLOW = Map.of(
            "CREATED", 0, "PAID", 1, "SHIPPED", 2, "DELIVERED", 3
    );

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "level1-consumer-group-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        ObjectMapper mapper = new ObjectMapper();

        System.out.println("=".repeat(60));
        System.out.println("  LEVEL 1: Single Partition Consumer");
        System.out.println("  Expecting messages in perfect send order");
        System.out.println("=".repeat(60));
        System.out.println();

        Map<String, Integer> orderTracker = new HashMap<>();
        int totalMessages = 0;
        boolean outOfOrder = false;

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
                    String orderId = data.get("order_id").asText();
                    String status = data.get("status").asText();
                    totalMessages++;

                    int lastSeq = orderTracker.getOrDefault(orderId, -1);
                    int currentSeq = EXPECTED_FLOW.get(status);
                    boolean isOrdered = (currentSeq == 0 && lastSeq == -1) || (currentSeq == lastSeq + 1);

                    if (!isOrdered) outOfOrder = true;
                    orderTracker.put(orderId, currentSeq);

                    String icon = isOrdered ? GREEN + "✓" + RESET : RED + "✗" + RESET;

                    System.out.printf("%sRECV%s  Order %s → %-10s | partition=%d offset=%d %s%n",
                            CYAN, RESET, orderId, status, record.partition(), record.offset(), icon);
                }
            }
        }

        // Print summary
        System.out.println();
        System.out.println("=".repeat(60));
        if (!outOfOrder) {
            System.out.printf("  %s%sRESULT: All messages arrived in perfect order!%s%n", GREEN, BOLD, RESET);
            System.out.println();
            System.out.println("  Why? Because with 1 partition, Kafka writes all messages");
            System.out.println("  to a single append-only log. Consumers read from that log");
            System.out.println("  sequentially — what goes in first, comes out first.");
        } else {
            System.out.printf("  %sRESULT: Some messages arrived out of order!%s%n", RED, RESET);
        }
        System.out.println("=".repeat(60));
        System.out.println();
        System.out.printf("  Total messages consumed: %d%n", totalMessages);
        System.out.println();
        System.out.printf("  %sLimitation:%s A single partition means a single consumer%n", YELLOW, RESET);
        System.out.println("  can process messages — no parallelism. This becomes a");
        System.out.println("  bottleneck as throughput grows.");
        System.out.println();
        System.out.println("  → Head to level2 to see what happens when");
        System.out.println("    we add more partitions for speed!");
        System.out.println();
    }
}

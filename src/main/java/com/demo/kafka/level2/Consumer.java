package com.demo.kafka.level2;

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
 * Level 2 — Multi-Partition Threaded Consumer
 * =============================================
 * Consumes from a 4-partition topic with multiple threads.
 *
 * Key insight: Messages for the SAME order can end up on different
 * partitions. When consumed in parallel, order lifecycle events
 * arrive jumbled — you might process SHIPPED before PAID.
 *
 * Run: mvn exec:java -Dexec.mainClass="com.demo.kafka.level2.Consumer"
 */
public class Consumer {

    private static final String TOPIC = "orders-level2";
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
        System.out.println("=".repeat(65));
        System.out.println("  LEVEL 2: Multi-Partition Threaded Consumer");
        System.out.println("  Consuming with multiple threads from 4 partitions");
        System.out.println("=".repeat(65));
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
        System.out.println("=".repeat(65));
        System.out.printf("  %sORDERING ANALYSIS%s%n", BOLD, RESET);
        System.out.println("-".repeat(65));

        analyzeOrdering();
    }

    private static void consumePartition(int threadId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "level2-consumer-group-" + UUID.randomUUID());
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
                    allMessages.add(entry);

                    synchronized (System.out) {
                        System.out.printf("%sRECV%s  [Thread-%d] Order %s → %-10s | partition=%d offset=%d%n",
                                CYAN, RESET, threadId,
                                data.get("order_id").asText(),
                                data.get("status").asText(),
                                record.partition(), record.offset());
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void analyzeOrdering() {
        Map<String, List<String>> orderEvents = new TreeMap<>();
        for (Map<String, Object> entry : allMessages) {
            String oid = (String) entry.get("order_id");
            orderEvents.computeIfAbsent(oid, k -> new ArrayList<>()).add((String) entry.get("status"));
        }

        List<String> expectedFlow = List.of("CREATED", "PAID", "SHIPPED", "DELIVERED");
        int orderedCount = 0;
        int brokenCount = 0;

        for (Map.Entry<String, List<String>> e : orderEvents.entrySet()) {
            if (e.getValue().equals(expectedFlow)) {
                orderedCount++;
            } else {
                brokenCount++;
                System.out.printf("  %s✗%s %s: %s%n", RED, RESET, e.getKey(), String.join(" → ", e.getValue()));
            }
        }

        System.out.println();
        if (brokenCount > 0) {
            System.out.printf("  %s✓ In order:     %d orders%s%n", GREEN, orderedCount, RESET);
            System.out.printf("  %s✗ Out of order: %d orders%s%n", RED, brokenCount, RESET);
            System.out.println();
            System.out.printf("  %s%sRESULT: Ordering is BROKEN!%s%n", RED, BOLD, RESET);
            System.out.println();
            System.out.println("  Why? Without a partition key, Kafka round-robins messages");
            System.out.println("  across partitions. Events for the same order land on");
            System.out.println("  different partitions and are consumed by different threads");
            System.out.println("  — there's no guarantee about cross-partition ordering.");
        } else {
            System.out.printf("  %s✓ All %d orders arrived in order%s%n", GREEN, orderedCount, RESET);
            System.out.println();
            System.out.println("  You got lucky! But this is NOT guaranteed. Run it again");
            System.out.println("  and you'll likely see ordering violations.");
        }

        System.out.println();
        System.out.printf("  %sThe tradeoff:%s%n", YELLOW, RESET);
        System.out.println("    Single partition  → perfect order, limited throughput");
        System.out.println("    Multi partition   → high throughput, broken order");
        System.out.println();
        System.out.println("  → Head to level3 to see how we");
        System.out.println("    get BOTH speed AND ordering!");
        System.out.println();
    }
}

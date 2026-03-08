package com.demo.kafka.level2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Level 2 — Multi-Partition Threaded Producer
 * =============================================
 * Demonstrates what happens when we add partitions and threads for speed.
 *
 * Same scenario: e-commerce order lifecycle events.
 * But now the topic has 4 partitions and we use 4 producer threads.
 *
 * Result: Much faster throughput, but message ordering is NO LONGER
 * guaranteed across partitions. You may see SHIPPED before PAID!
 *
 * Run: mvn exec:java -Dexec.mainClass="com.demo.kafka.level2.Producer"
 */
public class Producer {

    private static final String TOPIC = "orders-level2";
    private static final String BROKER = "localhost:9092";
    private static final int NUM_THREADS = 4;
    private static final String[] ORDER_STAGES = {"CREATED", "PAID", "SHIPPED", "DELIVERED"};

    // ANSI colors
    private static final String GREEN = "\u001B[92m";
    private static final String YELLOW = "\u001B[93m";
    private static final String RESET = "\u001B[0m";

    public static void main(String[] args) throws Exception {
        System.out.println("=".repeat(65));
        System.out.println("  LEVEL 2: Multi-Partition Threaded Producer");
        System.out.println("  Topic has 4 partitions + 4 producer threads");
        System.out.printf("  %sWARNING: Ordering will be LOST across partitions!%s%n", YELLOW, RESET);
        System.out.println("=".repeat(65));
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

        // Launch all threads simultaneously
        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();

        long elapsed = System.currentTimeMillis() - startTime;

        System.out.println();
        System.out.println("-".repeat(65));
        System.out.printf("  All messages sent in %dms using %d threads.%n", elapsed, NUM_THREADS);
        System.out.println("  Messages are scattered across 4 partitions — no key was used,");
        System.out.println("  so Kafka round-robined them.");
        System.out.println();
        System.out.println("  Now run the Consumer to see the ordering chaos!");
        System.out.println("-".repeat(65));
    }

    private static void produceOrders(int threadId, int startOrder, int endOrder) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        ObjectMapper mapper = new ObjectMapper();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int orderId = startOrder; orderId < endOrder; orderId++) {
                for (int seq = 0; seq < ORDER_STAGES.length; seq++) {
                    String stage = ORDER_STAGES[seq];
                    String orderKey = String.format("ORD-%03d", orderId);

                    ObjectNode message = mapper.createObjectNode();
                    message.put("order_id", orderKey);
                    message.put("status", stage);
                    message.put("sequence", seq);
                    message.put("thread_id", threadId);
                    message.put("timestamp", System.currentTimeMillis());

                    // No key specified → Kafka round-robins across partitions!
                    ProducerRecord<String, String> record =
                            new ProducerRecord<>(TOPIC, mapper.writeValueAsString(message));

                    RecordMetadata metadata = producer.send(record).get();

                    synchronized (System.out) {
                        System.out.printf("%sSENT%s  [Thread-%d] Order %s → %-10s | partition=%d offset=%d%n",
                                GREEN, RESET, threadId, orderKey, stage, metadata.partition(), metadata.offset());
                    }

                    Thread.sleep(50);
                }
            }
        }
    }
}

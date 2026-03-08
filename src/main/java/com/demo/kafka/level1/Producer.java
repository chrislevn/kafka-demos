package com.demo.kafka.level1;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Level 1 — Single Partition Producer
 * ====================================
 * Demonstrates Kafka's ordering guarantee within a single partition.
 *
 * Scenario: An e-commerce system processes order lifecycle events.
 * Each order goes through: CREATED → PAID → SHIPPED → DELIVERED
 *
 * With a single partition, ALL events land in one queue — Kafka guarantees
 * they come out in the exact order they went in.
 *
 * Run: mvn exec:java -Dexec.mainClass="com.demo.kafka.level1.Producer"
 */
public class Producer {

    private static final String TOPIC = "orders-level1";
    private static final String BROKER = "localhost:9092";
    private static final String[] ORDER_STAGES = {"CREATED", "PAID", "SHIPPED", "DELIVERED"};

    // ANSI colors
    private static final String GREEN = "\u001B[92m";
    private static final String RESET = "\u001B[0m";

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        ObjectMapper mapper = new ObjectMapper();

        System.out.println("=".repeat(60));
        System.out.println("  LEVEL 1: Single Partition Producer");
        System.out.println("  Topic has 1 partition → total ordering guaranteed");
        System.out.println("=".repeat(60));
        System.out.println();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            // Simulate 5 orders, each going through all lifecycle stages
            for (int orderId = 1; orderId <= 5; orderId++) {
                for (int seq = 0; seq < ORDER_STAGES.length; seq++) {
                    String stage = ORDER_STAGES[seq];
                    String orderKey = String.format("ORD-%03d", orderId);

                    ObjectNode message = mapper.createObjectNode();
                    message.put("order_id", orderKey);
                    message.put("status", stage);
                    message.put("sequence", seq);
                    message.put("timestamp", System.currentTimeMillis());

                    // No key → all messages go to the single partition
                    ProducerRecord<String, String> record =
                            new ProducerRecord<>(TOPIC, mapper.writeValueAsString(message));

                    RecordMetadata metadata = producer.send(record).get();

                    System.out.printf("%sSENT%s  Order %s → %-10s | partition=%d offset=%d%n",
                            GREEN, RESET, orderKey, stage, metadata.partition(), metadata.offset());

                    Thread.sleep(100);
                }
                System.out.println();
            }
        }

        System.out.println("-".repeat(60));
        System.out.println("All messages sent to a SINGLE partition.");
        System.out.println("Now run the Consumer to see them arrive in perfect order!");
        System.out.println("-".repeat(60));
    }
}

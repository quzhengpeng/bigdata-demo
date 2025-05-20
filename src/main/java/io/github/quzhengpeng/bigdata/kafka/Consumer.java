package io.github.quzhengpeng.bigdata.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;


public class Consumer implements Runnable {

    private static final String GROUP_ID = "GROUP_1";
    private final KafkaConsumer<String, String> consumer;

    public Consumer(String topic) {
        Properties props = new Properties();
        String brokers = "localhost:9092";
        props.put("bootstrap.servers", brokers);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("group.id", GROUP_ID);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("max.poll.records", 1000);
        props.put("auto.offset.reset", "earliest");

        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList(topic));
    }

    @Override
    public void run() {
        boolean flag = true;
        long maxOffset = -1;
        try {
            while (flag) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
                if (!consumerRecords.isEmpty() && consumerRecords.count() > 0) {
                    for (ConsumerRecord<String, String> record : consumerRecords) {
                        System.out.println("Received message: (" + record.key() + "," + record.value() + ") at offset " + record.offset());
                        maxOffset = Math.max(record.offset(), maxOffset);
                    }
                } else if (maxOffset > 100) {
                    flag = false;
                } else {
                    Thread.sleep(1000);
                }
            }
        } catch (Exception e) {
            e.fillInStackTrace();
        } finally {
            consumer.close();
        }
    }

    public static void main(String[] args) {
        Consumer consumer = new Consumer("kafka_test");
        Thread thread = new Thread(consumer);
        thread.start();
    }
}

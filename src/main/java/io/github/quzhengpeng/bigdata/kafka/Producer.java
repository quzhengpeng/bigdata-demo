package io.github.quzhengpeng.bigdata.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class Producer implements Runnable {
    private final KafkaProducer<String, String> producer;
    private final String topic;

    private Producer(String topic) {
        Properties props = new Properties();
        String brokers = "localhost:9092";
        props.put("bootstrap.servers", brokers);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        // acks=0：生产者不会等待 kafka 的响应。
        // acks=1：kafka 会把这条消息写到本地日志文件中，但是不会等待集群中其他机器的成功响应。
        // acks=all：leader 会等待所有的 follower 同步完成，确保消息不会丢失。除非 kafka 集群中所有机器挂掉，这是最强的可用性保证。
        props.put("acks", "all");
        // 如果 retries 的值大于 0 ，客户端会在消息发送失败时重新发送。
        props.put("retries", 0);
        // 当多条消息需要发送到同一个分区时，生产者会尝试合并网络请求。这会提高 client 和生产者的效率。
        props.put("batch.size", 16384);

        this.topic = topic;
        this.producer = new KafkaProducer<>(props);
    }

    @Override
    public void run() {
        int messageNum = 1;
        boolean flag = true;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            while (flag) {
                Date date = new Date();
                String messageStr = "[" + simpleDateFormat.format(date) + "] This is message " + messageNum++;
                producer.send(new ProducerRecord<>(topic, simpleDateFormat.format(date), messageStr));
                Thread.sleep(1000);
                if (messageNum > 1000) {
                    flag = false;
                }
            }
        } catch (Exception e) {
            e.fillInStackTrace();
        } finally {
            producer.close();
        }
    }

    public static void main(String[] args) {
        Producer producer = new Producer("kafka_test");
        Thread thread = new Thread(producer);
        thread.start();
    }
}

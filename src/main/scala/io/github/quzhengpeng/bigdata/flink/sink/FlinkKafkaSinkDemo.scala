package io.github.quzhengpeng.bigdata.flink.sink

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema.KafkaSinkContext
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala._
import org.apache.kafka.clients.producer.ProducerRecord

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object FlinkKafkaSinkDemo {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val brokers = "localhost:9092";
    val source_topic = "kafka_test"
    val sink_topic = "flink_kafka_test"
    val GROUP_ID = "GROUP_FlinkKafkaDemo"

    // 从 kafka 读取数据
    val kafkaSourceBuilder = KafkaSource.builder()
      .setBootstrapServers(brokers)
      .setGroupId(GROUP_ID)
      .setTopics(source_topic)
      .setStartingOffsets(OffsetsInitializer.latest())
      // 指定反序列化器，这个是反序列化 value
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    val kafkaSource = env.fromSource(kafkaSourceBuilder, WatermarkStrategy.noWatermarks(), "KafkaSource")
    kafkaSource.print()

    val kafkaSinkBuilder = KafkaSink.builder[String]
      .setBootstrapServers(brokers)
      .setRecordSerializer(
        KafkaRecordSerializationSchema.builder()
          .setTopic(sink_topic)
          .setKeySerializationSchema(new SimpleStringSchema())
          .setValueSerializationSchema(new SimpleStringSchema())
          .build()
      )
      .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .build()

    // 这种方式可以自定义 key，更多的可以参考这篇文章 https://blog.csdn.net/Milesian/article/details/147422655
    val kafkaSinkBuilder_key = KafkaSink.builder[String]
      .setBootstrapServers(brokers)
      .setRecordSerializer(
        new KafkaRecordSerializationSchema[String] {
          override def serialize(element: String, context: KafkaSinkContext, timestamp: java.lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
            val key = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME).getBytes("UTF-8")
            val value = element.getBytes("UTF-8")
            new ProducerRecord[Array[Byte], Array[Byte]]("kafka_test", key, value)
          }
        }
      )
      .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .build()


    kafkaSource.sinkTo(kafkaSinkBuilder)
    env.execute("FlinkKafkaSinkDemo")
  }
}
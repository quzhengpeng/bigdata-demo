package io.github.quzhengpeng.bigdata.flink.source

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

object FlinkKafkaSourceDemo {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val brokers = "localhost:9092";
    val topic = "flink_kafka_test"
    val GROUP_ID = "GROUP_FlinkKafkaDemo"

    // 从 kafka 读取数据
    val kafkaSourceBuilder = KafkaSource.builder()
      .setBootstrapServers(brokers)
      .setGroupId(GROUP_ID)
      .setTopics(topic)
      .setStartingOffsets(OffsetsInitializer.latest())
      // 指定反序列化器，这个是反序列化 value
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    val kafkaSource = env.fromSource(kafkaSourceBuilder, WatermarkStrategy.noWatermarks(), "KafkaSource")
      .map(_.trim.substring(22))
      .filter(_ != "")
      .flatMap(_.split("\\s"))
      .map((_, 1))
      .keyBy(_._1)
      .sum(1)

    kafkaSource.print()
    env.execute("FlinkKafkaSourceDemo")
  }
}

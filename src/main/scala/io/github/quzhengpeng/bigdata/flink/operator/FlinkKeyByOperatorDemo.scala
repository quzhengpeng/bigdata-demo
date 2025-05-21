package io.github.quzhengpeng.bigdata.flink.operator

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.api.java.functions.KeySelector

object FlinkKeyByOperatorDemo {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 生成 DataStream 并指定事件时间的字段
    val dataStream = env.fromElements(("hello", 1), ("flink", 2), ("hello", 3), ("flink", 5))
      .assignAscendingTimestamps(_._2)

    dataStream.keyBy(value => value._1)
      .sum(1)
      .print("KeyedStream.keyBy(fun: T => K)")

    dataStream.keyBy(new KeySelector[(String, Int), String] {
      override def getKey(value: (String, Int)): String = {
        value._1
      }
    })
      .sum(1)
      .print("keyedStream.keyBy(fun: KeySelector[T, K])")

    env.execute("FlinkKeyByOperatorDemo")
  }
}

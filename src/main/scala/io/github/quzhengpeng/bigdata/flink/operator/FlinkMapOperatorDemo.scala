package io.github.quzhengpeng.bigdata.flink.operator

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

object FlinkMapOperatorDemo {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 生成 DataStream 并指定事件时间的字段
    val dataStream = env.fromElements(("hello", 1), ("flink", 2), ("hello", 3), ("flink", 5))
      .assignAscendingTimestamps(_._2)

    dataStream.map(x => (x._1, x._2 * 10))
      .print("DataStream.map(fun: T => R)")

    dataStream.map(new MapFunction[(String, Int), (String, Int)] {
      override def map(value: (String, Int)): (String, Int) = {
        (value._1, value._2 * 10)
      }
    })
      .print("DataStream.map(mapper: MapFunction[T, R]")

    env.execute("FlinkMapOperatorDemo")
  }
}

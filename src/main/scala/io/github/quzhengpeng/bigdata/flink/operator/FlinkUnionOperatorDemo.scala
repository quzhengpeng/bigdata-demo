package io.github.quzhengpeng.bigdata.flink.operator

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

object FlinkUnionOperatorDemo {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 生成 DataStream 并指定事件时间的字段
    val dataStream_1 = env.fromElements(("hello", 1), ("flink", 2), ("hello", 3), ("flink", 5))
      .assignAscendingTimestamps(_._2)

    val dataStream_2 = env.fromElements(("hello", 7), ("flink", 9))

    dataStream_1.union(dataStream_2)
      .print("DataStream.union(dataStreams: DataStream[T]*)")

    env.execute("FlinkUnionOperatorDemo")
  }
}

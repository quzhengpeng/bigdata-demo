package io.github.quzhengpeng.bigdata.flink.operator

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

object FlinkReduceOperatorDemo {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 生成 KeyedStream 并指定事件时间的字段
    val keyedStream = env.fromElements(("hello", 1), ("flink", 2), ("hello", 3), ("flink", 5))
      .assignAscendingTimestamps(_._2)
      .keyBy(_._1)

    keyedStream.reduce((x, y) => (x._1, x._2 + y._2))
      .print("KeyedStream.reduce(fun: (T, T) => T)")

    keyedStream.reduce(new ReduceFunction[(String, Int)] {
      override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
        (value1._1, value1._2 + value2._2)
      }
    })
      .print("KeyedStream.reduce(reducer: ReduceFunction[T])")

    env.execute("FlinkReduceOperatorDemo")
  }
}

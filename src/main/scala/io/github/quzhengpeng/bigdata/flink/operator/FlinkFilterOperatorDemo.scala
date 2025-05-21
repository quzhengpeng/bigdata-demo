package io.github.quzhengpeng.bigdata.flink.operator

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

object FlinkFilterOperatorDemo {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 生成 DataStream
    val dataStream = env.fromElements(("hello", 1), ("flink", 2), ("hello", 3), ("flink", 5))
      .assignAscendingTimestamps(_._2)

    dataStream.filter(_._1 == "hello")
      .print("DataStream.filter(fun: T => Boolean)")

    dataStream.filter(new FilterFunction[(String, Int)]() {
        override def filter(value: (String, Int)): Boolean = {
          value._1.equals("hello")
        }
      })
      .print("DataStream.filter(filter: FilterFunction[T])")

    env.execute("FlinkFilterOperatorDemo")
  }
}

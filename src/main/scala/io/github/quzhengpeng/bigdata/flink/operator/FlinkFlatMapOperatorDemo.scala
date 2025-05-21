package io.github.quzhengpeng.bigdata.flink.operator

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector

object FlinkFlatMapOperatorDemo {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 生成 DataStream
    val dataStream = env.fromElements("hello flink hello flink")

    dataStream.flatMap(value => value.split(" "))
      .print("DataStream.flatMap(fun: T => TraversableOnce[R])")

    dataStream.flatMap((value, out: Collector[String]) => {
      for (elem <- value.split(" ")) {
        out.collect(elem)
      }
    })
      .print("DataStream.flatMap(fun: (T, Collector[R]) => Unit)")


    dataStream.flatMap(new FlatMapFunction[String, String] {
      override def flatMap(value: String, out: Collector[String]): Unit = {
        for (elem <- value.split(" ")) {
          out.collect(elem)
        }
      }
    })
      .print("flatMapper: FlatMapFunction[T, R]")

    env.execute("FlinkFlatMapOperatorDemo")
  }
}

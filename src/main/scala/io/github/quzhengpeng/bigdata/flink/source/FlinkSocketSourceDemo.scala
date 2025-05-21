package io.github.quzhengpeng.bigdata.flink.source

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

object FlinkSocketSourceDemo {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // nc -l 8888
    val socketSource = env.socketTextStream("localhost",8888)
      .map(_.trim)
      .filter(_ != "")
      .flatMap(_.split("\\s"))
      .map((_, 1))
      .keyBy(_._1)
      .sum(1)

    socketSource.print()
    env.execute("FlinkSocketSourceDemo")
  }
}

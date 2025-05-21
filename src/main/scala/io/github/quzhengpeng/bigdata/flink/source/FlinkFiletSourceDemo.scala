package io.github.quzhengpeng.bigdata.flink.source

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

object FlinkFiletSourceDemo {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val fileSource = env.readTextFile("./src/main/resources/FlinkFileSourceDemo.txt")
      .map(_.trim)
      .filter(_ != "")
      .flatMap(_.split("\\s"))
      .map((_, 1))
      .keyBy(_._1)
      .sum(1)

    fileSource.print()
    env.execute("FlinkFileSourceDemo")
  }
}

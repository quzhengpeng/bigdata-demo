package io.github.quzhengpeng.bigdata.flink.source

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}

object FlinkCollectionSourceDemo {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    def wordCount(input: DataStream[String]) = {
      input
        .map(_.trim)
        .filter(_ != "")
        .flatMap(_.split("\\s"))
        .map((_, 1))
        .keyBy(_._1)
        .sum(1)
        .print()
    }

    // Elements fromElements 实际也是调用 fromCollection 方法
    wordCount(env.fromElements("Elements", "Elements"))

    // Collection Array
    wordCount(env.fromCollection(Array("Array", "Array")))

    // Collection List
    wordCount(env.fromCollection(List("List", "List")))

    // Collection Seq
    wordCount(env.fromCollection(Seq("Seq", "Seq")))

    // Sequence
    env.fromSequence(1, 2).print()

    env.execute("FlinkCollectionSourceDemo")
  }
}

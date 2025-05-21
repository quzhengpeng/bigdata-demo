package io.github.quzhengpeng.bigdata.flink.operator

import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object FlinkJoinOperatorDemo {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 生成 DataStream 并指定事件时间的字段
    val dataStream_1 = env.fromElements(("hello", 1), ("flink", 2), ("hello", 3), ("flink", 5), ("hello", 7), ("flink", 9))
      .assignAscendingTimestamps(_._2)

    val dataStream_2 = env.fromElements(("hello", 2), ("flink", 3), ("hello", 4), ("flink", 6), ("hello", 8), ("flink", 10))
      .assignAscendingTimestamps(_._2)

    dataStream_1.join(dataStream_2)
      .where(_._1).equalTo(_._1)
      .window(TumblingEventTimeWindows.of(Time.milliseconds(5)))
      .apply((s1, s2) => (s1._1, s1._2, s2._1, s2._2))
      .print("DataStream.join()")

    dataStream_1.keyBy(_._1)
      .intervalJoin(dataStream_2.keyBy(_._1))
      .between(Time.milliseconds(-1), Time.milliseconds(2))
      .process((
                 left: (String, Int),
                 right: (String, Int),
                 ctx: ProcessJoinFunction[(String, Int), (String, Int), (String, Int, String, Int)]#Context,
                 out: Collector[(String, Int, String, Int)]) => {
        val timestamp = "[" + ctx.getTimestamp + "] <- "
        val left_timestamp = "[" + ctx.getLeftTimestamp + "] "
        val right_timestamp = "[" + ctx.getRightTimestamp + "] "
        out.collect((timestamp + left_timestamp + left._1, left._2, right_timestamp + right._1, right._2))
      })
      .print("DataStream.intervalJoin()")

    env.execute("FlinkJoinOperatorDemo")
  }
}

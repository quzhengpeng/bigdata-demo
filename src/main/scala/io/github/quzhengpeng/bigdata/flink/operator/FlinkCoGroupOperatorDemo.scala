package io.github.quzhengpeng.bigdata.flink.operator

import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.lang
import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`

object FlinkCoGroupOperatorDemo {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 生成 DataStream 并指定事件时间的字段
    val dataStream_1 = env.fromElements(("hello", 1), ("flink", 2), ("hello", 3), ("flink", 5), ("hello", 7), ("flink", 9))
      .assignAscendingTimestamps(_._2)
      .setParallelism(1)

    val dataStream_2 = env.fromElements(("hello", 2), ("flink", 3), ("hello", 4), ("flink", 6), ("hello", 8), ("flink", 10))
      .assignAscendingTimestamps(_._2)
      .setParallelism(1)

    val coGroupedStream = dataStream_1.coGroup(dataStream_2)
      .where(_._1)
      .equalTo(_._1)
      .window(TumblingEventTimeWindows.of(Time.milliseconds(5)))

    coGroupedStream.apply((first: Iterator[(String, Int)], second: Iterator[(String, Int)]) => {
        val first_list = first.toList
        val second_list = second.toList

        // 左外联接
        if (second_list.isEmpty)
          for (first_elem <- first_list)
            yield (first_elem._1, first_elem._2, "", 0)
        else
          for (first_elem <- first_list; second_elem <- second_list)
            yield (first_elem._1, first_elem._2, second_elem._1, second_elem._2)
      })
      .flatMap(_.toList)
      .print("CoGroupedStreams.coGroup.apply(fun: (Iterator[T1], Iterator[T2]) => O)")

    coGroupedStream.apply((first: Iterator[(String, Int)], second: Iterator[(String, Int)], out: Collector[(String, Int, String, Int)]) => {
        val first_list = first.toList
        val second_list = second.toList

        // 左外联接
        if (second_list.isEmpty)
          for (first_elem <- first_list)
            out.collect(first_elem._1, first_elem._2, "", 0)
        else
          for (first_elem <- first_list; second_elem <- second_list)
            out.collect(first_elem._1, first_elem._2, second_elem._1, second_elem._2)
      })
      .print("CoGroupedStreams.coGroup.apply(fun: (Iterator[T1], Iterator[T2], Collector[O]) => Unit)")


    coGroupedStream.apply(new CoGroupFunction[(String, Int), (String, Int), (String, Int, String, Int)] {
        override def coGroup(first: lang.Iterable[(String, Int)], second: lang.Iterable[(String, Int)], out: Collector[(String, Int, String, Int)]): Unit = {
          val first_list = first.toList
          val second_list = second.toList

          // 右外联接
          if (first_list.isEmpty)
            for (second_elem <- second_list)
              out.collect("", 0, second_elem._1, second_elem._2)
          else
            for (first_elem <- first_list; second_elem <- second_list)
              out.collect(first_elem._1, first_elem._2, second_elem._1, second_elem._2)
        }
      })
      .print("CoGroupedStreams.coGroup.apply(function: CoGroupFunction[T1, T2, T])")

    env.execute("FlinkCoGroupOperatorDemo")
  }
}

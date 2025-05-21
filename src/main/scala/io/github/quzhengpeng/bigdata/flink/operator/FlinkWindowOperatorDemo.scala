package io.github.quzhengpeng.bigdata.flink.operator

import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, ProcessAllWindowFunction, ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object FlinkWindowOperatorDemo {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 生成 DataStream 并指定事件时间的字段
    val dataStream = env.fromElements(("hello", 1), ("flink", 2), ("hello", 3), ("flink", 5), ("hello", 7), ("flink", 9))
      .assignAscendingTimestamps(_._2)

    // 生成 AllWindowedStream，使用滚动时间窗口，窗口大小为 5ms
    val allWindowedStream = dataStream
      .windowAll(TumblingEventTimeWindows.of(Time.milliseconds(5)))

    // 生成 WindowedStream，使用滚动时间窗口，窗口大小为 5ms
    val windowedStream = dataStream
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.milliseconds(5)))


    // WindowedStream 算子下的 reduce 操作
    windowedStream.reduce(new ReduceFunction[(String, Int)] {
      override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
        (value1._1, value1._2 + value2._2)
      }
    })
      .print("WindowedStream.reduce(function: ReduceFunction[T])")

    windowedStream
      .reduce((x, y) => (x._1, x._2 + y._2))
      .print("WindowedStream.reduce(function: (T, T) => T)")

    // WindowFunction[IN, OUT, KEY, W <: Window]
    windowedStream.reduce(new ReduceFunction[(String, Int)] {
      override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
        (value1._1, value1._2 + value2._2)
      }
    }, new WindowFunction[(String, Int), (String, Int), String, TimeWindow] {
      override def apply(key: String, window: TimeWindow, input: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
        // 虽然不知道原理是啥，但是这样确实会输出想要得到的结果
        for (elem <- input) {
          out.collect(key, elem._2)
        }
      }
    })
      .print("WindowedStream.reduce(preAggregator: ReduceFunction[T], function: WindowFunction[T, R, K, W])")

    windowedStream.reduce((x, y) => (x._1, x._2 + y._2),
      (key: String, _: TimeWindow, input: Iterable[(String, Int)], out: Collector[(String, Int)]) => {
        for (elem <- input) {
          out.collect(key, elem._2)
        }
      }
    )
      .print("WindowedStream.reduce(preAggregator: (T, T) => T, windowFunction: (K, W, Iterable[T], Collector[R]) => Unit)")

    // ProcessWindowFunction[IN, OUT, KEY, W <: Window]
    windowedStream.reduce(new ReduceFunction[(String, Int)] {
      override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
        (value1._1, value1._2 + value2._2)
      }
    },
      new ProcessWindowFunction[(String, Int), (String, Int), String, TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
          for (elem <- elements) {
            out.collect((key, elem._2))
          }
        }
      })
      .print("WindowedStream.reduce(preAggregator: (T, T) => T, function: ProcessWindowFunction[T, R, K, W])")


    // ProcessWindowFunction[IN, OUT, KEY, W <: Window]】
    windowedStream.reduce((x, y) => (x._1, x._2 + y._2),
      new ProcessWindowFunction[(String, Int), (String, Int), String, TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
          for (elem <- elements) {
            out.collect((key, elem._2))
          }
        }
      })
      .print("WindowedStream.reduce(preAggregator: ReduceFunction[T], function: ProcessWindowFunction[T, R, K, W])")

    // AllWindowedStream 算子下的 reduce 操作，类似 WindowedStream 下的 reduce() 操作，只是没有了 key 信息。
    allWindowedStream
      .reduce((x, y) => (x._1, x._2 + y._2))
      .print("AllWindowedStream.reduce()")


    // WindowedStream 算子下的 aggregate 操作
    // AggregateFunction<IN, ACC, OUT>
    val agg_func = new AggregateFunction[(String, Int), (String, Int), (String, Int)] {
      // ACC createAccumulator()
      override def createAccumulator(): (String, Int) = ("", 0)

      // ACC add(IN value, ACC accumulator)
      override def add(value: (String, Int), accumulator: (String, Int)): (String, Int) = {
        (value._1, value._2 + accumulator._2)
      }

      // OUT getResult(ACC accumulator)
      override def getResult(accumulator: (String, Int)): (String, Int) = accumulator

      // ACC merge(ACC a, ACC b)
      override def merge(a: (String, Int), b: (String, Int)): (String, Int) = {
        (a._1, a._2 + b._2)
      }
    }

    windowedStream.aggregate(agg_func)
      .print("WindowedStream.aggregate(aggregateFunction: AggregateFunction[T, ACC, R])")

    // WindowFunction[IN, OUT, KEY, W <: Window]
    windowedStream.aggregate(agg_func, new WindowFunction[(String, Int), (String, Int), String, TimeWindow] {
      override def apply(key: String, window: TimeWindow, input: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
        val start_dt = window.getStart.toString
        val end_dt = window.getEnd.toString
        val window_time = "[" + start_dt + "~" + end_dt + "] "
        input.map(value => (window_time + value._1, value._2))
          .foreach(elem => out.collect(elem))
      }
    })
      .print("WindowedStream.aggregate(preAggregator: AggregateFunction[T, ACC, V], windowFunction: WindowFunction[V, R, K, W])")

    windowedStream.aggregate(agg_func, (_: String, window: TimeWindow, input: Iterable[(String, Int)], out: Collector[(String, Int)]) => {
      val start_dt = window.getStart.toString
      val end_dt = window.getEnd.toString
      val window_time = "[" + start_dt + "~" + end_dt + "] "
      input.map(value => (window_time + value._1, value._2))
        .foreach(elem => out.collect(elem))
    })
      .print("WindowedStream.aggregate(preAggregator: AggregateFunction[T, ACC, V], windowFunction: (K, W, Iterable[V], Collector[R]) => Unit)")

    // ProcessWindowFunction[IN, OUT, KEY, W <: Window]
    windowedStream.aggregate(agg_func, new ProcessWindowFunction[(String, Int), (String, Int), String, TimeWindow] {
      override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
        val start_dt = context.window.getStart.toString
        val end_dt = context.window.getEnd.toString
        val window_time = "[" + start_dt + "~" + end_dt + "] "
        elements.map(value => (window_time + value._1, value._2))
          .foreach(elem => out.collect(elem))
      }
    })
      .print("WindowedStream.aggregate(preAggregator: AggregateFunction[T, ACC, V], windowFunction: ProcessWindowFunction[V, R, K, W]")

    // AllWindowedStream 算子下的 aggregate 操作，和 WindowedStream 算子下的 aggregate 操作相同
    allWindowedStream.aggregate(agg_func)
      .print("AllWindowedStream.aggregate(aggregateFunction: AggregateFunction[T, ACC, R])")


    // WindowedStream 算子下 sum min/minBy max/maxBy 方法
    windowedStream.sum(1)
    windowedStream.max(1)
    windowedStream.maxBy(1)
    windowedStream.min(0)
    windowedStream.minBy(1)

    // AllWindowedStream 算子下 sum min/minBy max/maxBy 方法
    allWindowedStream.sum(1)
    allWindowedStream.max(1)
    allWindowedStream.maxBy(1)
    allWindowedStream.min(1)
    allWindowedStream.minBy(1)

    // WindowedStream 算子下的 apply 操作
    // WindowFunction[IN, OUT, KEY, W <: Window]
    windowedStream.apply(new WindowFunction[(String, Int), (String, Int), String, TimeWindow] {
      override def apply(key: String, window: TimeWindow, input: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
        val start_dt = window.getStart.toString
        val end_dt = window.getEnd.toString
        val window_time = "[" + start_dt + "~" + end_dt + "] "
        val sum = input.map(value => (window_time + value._1, value._2))
          .reduce((x, y) => (x._1, x._2 + y._2))
        out.collect(sum)
      }
    })
      .print("WindowedStream.apply(function: WindowFunction[T, R, K, W])")

    windowedStream.apply((_: String, window: TimeWindow, input: Iterable[(String, Int)], out: Collector[(String, Int)]) => {
      val start_dt = window.getStart.toString
      val end_dt = window.getEnd.toString
      val window_time = "[" + start_dt + "~" + end_dt + "] "
      val sum = input.map(value => (window_time + value._1, value._2))
        .reduce((x, y) => (x._1, x._2 + y._2))
      out.collect(sum)
    })
      .print("WindowedStream.apply(function: (K, W, Iterable[T], Collector[R]) => Unit)")

    // AllWindowedStream 算子下的 apply 操作
    allWindowedStream.apply(new AllWindowFunction[(String, Int), (String, Int), TimeWindow] {
      override def apply(window: TimeWindow, input: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
        val start_dt = window.getStart.toString
        val end_dt = window.getEnd.toString
        val window_time = "[" + start_dt + "~" + end_dt + "] "
        val sum = input.map(value => (window_time + value._1, value._2))
          .reduce((x, y) => (x._1, x._2 + y._2))
        out.collect(sum)
      }
    })
      .print("AllWindowedStream.apply(function: AllWindowFunction[T, R, W])")


    // WindowedStream 算子下的 process 操作
    // ProcessWindowFunction[IN, OUT, KEY, W <: Window]
    windowedStream.process(new ProcessWindowFunction[(String, Int), (String, Int), String, TimeWindow] {
      override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
        val start_dt = context.window.getStart.toString
        val end_dt = context.window.getEnd.toString
        val window_time = "[" + start_dt + "~" + end_dt + "] [" + context.currentProcessingTime.toString + "] "
        val sum = elements.map(value => (window_time + value._1, value._2))
          .reduce((x, y) => (x._1, x._2 + y._2))
        out.collect(sum)
      }
    })
      .print("WindowedStream.process(function: ProcessWindowFunction[T, R, K, W])")

    // AllWindowedStream 算子下的 process 操作
    allWindowedStream.process(new ProcessAllWindowFunction[(String, Int), (String, Int), TimeWindow] {
      override def process(context: Context, elements: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
        val start_dt = context.window.getStart.toString
        val end_dt = context.window.getEnd.toString
        val window_time = "[" + start_dt + "~" + end_dt + "] "
        val sum = elements.map(value => (window_time + value._1, value._2))
          .reduce((x, y) => (x._1, x._2 + y._2))
        out.collect(sum)
      }
    })
      .print("AllWindowedStream.process(function: ProcessWindowFunction[T, R, K, W])")

    env.execute("FlinkWindowOperatorDemo")
  }
}

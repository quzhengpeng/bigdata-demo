package io.github.quzhengpeng.bigdata.flink.operator

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector

/**
 * Watermark 是按批生成的，默认间隔是 autoWatermarkInterval = 200。
 * 也就是说通过 fromElements 的方式，在读取数据量有限的情况下，会出现已经读完所有的数据了，但是 Watermark 还没来及刷新。
 * 这种情况 Watermark 就会停留在 -9223372036854775808 这个值，减缓数据读取速度可以避免这个问题。
 */
class SleepProcessFunction extends ProcessFunction[(String, Int), (String, Int)] {
  override def processElement(value: (String, Int), ctx: ProcessFunction[(String, Int), (String, Int)]#Context, out: Collector[(String, Int)]): Unit = {
    // 每输出一条数据暂停 0.5 秒
    Thread.sleep(500)
    out.collect(value)
  }
}

object FlinkProcessOperatorDemo {

  // 定义 sideOutput 标签
  val hello = new OutputTag[(String, Int)]("hello")
  val flink = new OutputTag[(String, Int)]("flink")

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 生成 DataStream 并指定事件时间的字段
    val dataStream = env.fromElements(("hello", 1), ("flink", 2), ("hello", 3), ("flink", 5), ("hello", 7), ("flink", 9))
      .assignAscendingTimestamps(_._2)
      .setParallelism(1)

    // 生成 KeyedStream
    val keyedStream = dataStream
      // 控制数据输出的速度
      .process(new SleepProcessFunction)
      .keyBy(_._1)

    val processedDataStream = keyedStream.process(new KeyedProcessFunction[String, (String, Int), (String, Long)] {
      // 定义状态变量
      private var count: ValueState[Long] = _

      override def open(parameters: Configuration): Unit = {
        val valueStateDescriptor = new ValueStateDescriptor[Long]("valueCount", createTypeInformation[Long])
        count = getRuntimeContext.getState[Long](valueStateDescriptor)
      }

      override def processElement(value: (String, Int), ctx: KeyedProcessFunction[String, (String, Int), (String, Long)]#Context, out: Collector[(String, Long)]): Unit = {

        //  状态编程：运行时上下文
        val runtimeContext = getRuntimeContext

        //  定时器
        val timerService = ctx.timerService

        // 获取当前数据的 key
        val currentKey = ctx.getCurrentKey

        // 获取当前数据事件时间
        val currentEventTime = ctx.timestamp

        // 获取当前数据处理时间
        val currentProcessingTime = timerService.currentProcessingTime

        // 获取当前数据水印时间
        val currentWatermark = timerService.currentWatermark

        // 注册定时器
        timerService.registerEventTimeTimer(currentEventTime + 10)

        if (currentKey == "hello") {
          //  侧输出流
          ctx.output(hello, value)
        } else {
          out.collect((currentKey, currentEventTime))
        }
      }

      override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, Int), (String, Long)]#OnTimerContext, out: Collector[(String, Long)]): Unit = {

        val currentKey = ctx.getCurrentKey
        val currentEventTime = ctx.timestamp
        val currentWatermark = ctx.timerService.currentWatermark
        count.update(count.value() + 1)

        System.out.println("onTimer : (key=" + currentKey + ", eventTime=" + currentEventTime + ", watermark = " + currentWatermark + ", event_time = " + currentEventTime + ", count=", count.value() + ")")
      }
    })

    processedDataStream.print("KeyedStream.process[R: TypeInformation](keyedProcessFunction: KeyedProcessFunction[K, T, R]): DataStream[R]")
    processedDataStream.getSideOutput(hello).print("KeyedStream.getSideOutput[X: TypeInformation](tag: OutputTag[X]): DataStream[X]")
    env.execute("FlinkProcessOperatorDemo")
  }
}
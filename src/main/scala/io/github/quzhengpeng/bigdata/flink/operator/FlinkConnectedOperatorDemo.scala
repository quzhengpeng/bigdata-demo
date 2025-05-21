package io.github.quzhengpeng.bigdata.flink.operator

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.{CoFlatMapFunction, CoMapFunction, CoProcessFunction, KeyedCoProcessFunction}
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector

object FlinkConnectedOperatorDemo {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 生成 DataStream 并指定事件时间的字段
    val dataStream_1 = env.fromElements(("hello", 1), ("flink", 2))
      .assignAscendingTimestamps(_._2)

    val dataStream_2 = env.fromElements(("hello", 2), ("flink", 4), ("apache", 10))
      .assignAscendingTimestamps(_._2)

    val keyedStream_1 = dataStream_1.keyBy(_._1)
    val keyedStream_2 = dataStream_2.keyBy(_._1)

    // map 方法
    dataStream_1.connect(dataStream_2)
      .keyBy(_._1, _._1)
      .map(new CoMapFunction[(String, Int), (String, Int), (String, Int)]() {

        override def map1(value: (String, Int)): (String, Int) = {
          ("map1 : " + value._1, value._2)
        }

        override def map2(value: (String, Int)): (String, Int) = {
          ("map2 : " + value._1, value._2)
        }
      })
      .print("ConnectedOperator.map(coMapper: CoMapFunction[IN1, IN2, R])")

    dataStream_1.connect(dataStream_2)
      .keyBy(_._1, _._1)
      .map(value => ("map1 : " + value._1, value._2), value => ("map2 : " + value._1, value._2))
      .print("ConnectedOperator.map(fun1: IN1 => R, fun2: IN2 => R)")

    // flatMap 方法
    dataStream_1.connect(dataStream_2)
      .keyBy(_._1, _._1)
      .flatMap(new CoFlatMapFunction[(String, Int), (String, Int), (String, Int)] {

        override def flatMap1(value: (String, Int), out: Collector[(String, Int)]): Unit = {
          out.collect(("flatMap1 : " + value._1, value._2))
        }

        override def flatMap2(value: (String, Int), out: Collector[(String, Int)]): Unit = {
          out.collect(("flatMap2 : " + value._1, value._2))
        }
      })
      .print("ConnectedOperator.flatMap(coFlatMapper: CoFlatMapFunction[IN1, IN2, R])")

    dataStream_1.connect(dataStream_2)
      .keyBy(_._1, _._1)
      .flatMap(
        (in, out: Collector[(String, Int)]) => out.collect("flatMap1 : " + in._1, in._2)
        , (in, out: Collector[(String, Int)]) => out.collect("flatMap2 : " + in._1, in._2)
      )
      .print("ConnectedOperator.flatMap(fun1: (IN1, Collector[R]) => Unit, fun2: (IN2, Collector[R]) => Unit))")

    dataStream_1.connect(dataStream_2)
      .keyBy(_._1, _._1)
      .flatMap(in => in._1, i => i._1)
      .print("ConnectedOperator.flatMap(fun1: IN1 => TraversableOnce[R], fun2: IN2 => TraversableOnce[R])")

    // process
    keyedStream_1.connect(keyedStream_2)
      .process(new KeyedCoProcessFunction[String, (String, Int), (String, Int), (String, Int)]() {

        override def processElement1(value: (String, Int), ctx: KeyedCoProcessFunction[String, (String, Int), (String, Int), (String, Int)]#Context, out: Collector[(String, Int)]): Unit = {
          out.collect("processElement1 : " + value._1 + "_" + ctx.timestamp, value._2)
        }

        override def processElement2(value: (String, Int), ctx: KeyedCoProcessFunction[String, (String, Int), (String, Int), (String, Int)]#Context, out: Collector[(String, Int)]): Unit = {
          out.collect("processElement2 : " + value._1 + "_" + ctx.timestamp, value._2)
        }
      })
      .print("ConnectedOperator.process(coProcessFunction: KeyedCoProcessFunction[IN1, IN2, R])")

    keyedStream_1.connect(keyedStream_2)
      .process(new CoProcessFunction[(String, Int), (String, Int), (String, Int)] {

        override def processElement1(value: (String, Int), ctx: CoProcessFunction[(String, Int), (String, Int), (String, Int)]#Context, out: Collector[(String, Int)]): Unit = {
          out.collect("processElement1 : " + value._1 + "_" + ctx.timestamp, value._2)
        }

        override def processElement2(value: (String, Int), ctx: CoProcessFunction[(String, Int), (String, Int), (String, Int)]#Context, out: Collector[(String, Int)]): Unit = {
          out.collect("processElement2 : " + value._1 + "_" + ctx.timestamp, value._2)
        }
      })
      .print("ConnectedOperator.process(coProcessFunction: CoProcessFunction[IN1, IN2, R])")

    dataStream_1.connect(dataStream_2)
      .process(new CoProcessFunction[(String, Int), (String, Int), (String, Int)] {

        override def processElement1(value: (String, Int), ctx: CoProcessFunction[(String, Int), (String, Int), (String, Int)]#Context, out: Collector[(String, Int)]): Unit = {
          out.collect("processElement1 : " + value._1 + "_" + ctx.timestamp, value._2)
        }

        override def processElement2(value: (String, Int), ctx: CoProcessFunction[(String, Int), (String, Int), (String, Int)]#Context, out: Collector[(String, Int)]): Unit = {
          out.collect("processElement2 : " + value._1 + "_" + ctx.timestamp, value._2)
        }
      })
      .print("ConnectedOperator.process(coProcessFunction: CoProcessFunction[IN1, IN2, R])")


    // Flink 状态控制
    keyedStream_1.connect(keyedStream_2)
      .process(new KeyedCoProcessFunction[String, (String, Int), (String, Int), (String, Int, Int)] {
        var elementState_1: ValueState[(String, Int)] = _
        var elementState_2: ValueState[(String, Int)] = _
        var timeState: ValueState[Long] = _

        override def open(parameters: Configuration): Unit = {
          elementState_1 = getRuntimeContext.getState(new ValueStateDescriptor[(String, Int)]("elementState_1", createTypeInformation[(String, Int)]))
          elementState_2 = getRuntimeContext.getState(new ValueStateDescriptor[(String, Int)]("elementState_2", createTypeInformation[(String, Int)]))
          timeState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timeState", createTypeInformation[Long]))
        }

        override def onTimer(timestamp: Long, ctx: KeyedCoProcessFunction[String, (String, Int), (String, Int), (String, Int, Int)]#OnTimerContext, out: Collector[(String, Int, Int)]): Unit = {
          // 这样为全外连接输出，删除else则是实现左连接，右连接则只输出else部分即可
          if (elementState_1.value() != null) {
            ctx.output(new OutputTag[String]("e1") {}, elementState_1.value()._1)
            out.collect(elementState_1.value()._1, elementState_1.value()._2, 0)
          } else {
            ctx.output(new OutputTag[String]("e1") {}, elementState_2.value()._1)
            out.collect(elementState_2.value()._1, 0, elementState_2.value()._2)
          }
          elementState_1.clear()
          elementState_2.clear()
          timeState.clear()
        }

        override def processElement1(value: (String, Int), ctx: KeyedCoProcessFunction[String, (String, Int), (String, Int), (String, Int, Int)]#Context, out: Collector[(String, Int, Int)]): Unit = {
          // 在 keyedStream_1 流中，判断 keyedStream_2 流是否有数据，没有则启用定时器 10s 后触发旁路输出
          if (elementState_2.value() == null) {
            // elementState_2 数据未到，先把 elementState_1 数据存入状态
            elementState_1.update(value)
            // 建立定时器， 10s 后触发
            val ts = value._2 + 10 * 1000L
            ctx.timerService().registerEventTimeTimer(ts)
            timeState.update(ts)
          } else {
            // elementState_2 数据已到，直接输入到主流
            out.collect((value._1, value._2, elementState_2.value()._2))
            // 删除定时器，如 elementState_2 流先到，elementState_2 流建立了的定时器，在这里删除
            ctx.timerService().deleteEventTimeTimer(timeState.value())
            // 清空状态，注意清空的是 elementState_2 状态
            elementState_2.clear()
            timeState.clear()
          }
        }

        override def processElement2(value: (String, Int), ctx: KeyedCoProcessFunction[String, (String, Int), (String, Int), (String, Int, Int)]#Context, out: Collector[(String, Int, Int)]): Unit = {
          // 在 keyedStream_2 流中，判断 keyedStream_1 流是否有数据，没有则启用定时器 10s 后触发旁路输出
          if (elementState_1.value() == null) {
            // elementState_1 数据未到，先把 elementState_2 数据存入状态
            elementState_2.update(value)
            // 建立定时器， 10s 后触发
            val ts = value._2 + 10 * 1000L
            ctx.timerService().registerEventTimeTimer(ts)
            timeState.update(ts)
          } else {
            // elementState_1 数据已到，直接输入到主流
            out.collect((value._1, elementState_1.value()._2, value._2))
            // 删除定时器，如 elementState_1 流先到，elementState_1 流建立了的定时器，在这里删除
            ctx.timerService().deleteEventTimeTimer(timeState.value())
            // 清空状态，注意清空的是 elementState_1 状态
            elementState_1.clear()
            timeState.clear()
          }
        }
      }).print("ConnectedOperator.process() 实现双流 full join")

    env.execute("FlinkConnectedOperatorDemo")
  }
}

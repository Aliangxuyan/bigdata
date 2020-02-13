package com.lxy.orderpay_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * @author lxy
  *         2020/2/13
  */
object OrderTimeoutWithoutCEP {
  val orderTimeoutOutputTag = new OutputTag[OrderResult]("orderTimeout")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 1、读取订单数据
    val resource = getClass.getResource("/OrderLog.csv")
    val orderEventStream = env.readTextFile(resource.getPath)
      //    val orderEventStream = env.socketTextStream("localhost", 7777)
      .map(data => {
      val dataArray = data.split(",")
      OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
    })
      .assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.orderId)

    // 定义一个processfunction进行超时检测
    //    val timeoutWarningStream = orderEventStream.process(new OrderTimeoutWarning())

    val orderResultStream = orderEventStream.process(new OrderPayMatch())

    orderResultStream.print()
    orderResultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")

    env.execute("OrderTimeoutWithoutCEP   job")

  }


  class OrderPayMatch() extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {
    //  保存支付是否来过的状态
    lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]
    ("isPayed-state", classOf[Boolean]))

    // 保持定时器的时间戳为状态
    lazy val timerState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]
    ("timer-state", classOf[Long]))

    override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
      val isPayed = isPayedState.value()
      val timerTs = timerState.value()

      // 根据事件的类型进行分类判断，做不同的处理逻辑
      if (value.eventType == "create") {
        // 如果是create 事件，接下来判断pay  是否来过，
        if (isPayed) {
          // 如果已经pay 过，匹配成功输出主流数据，清空状态
          out.collect(OrderResult(ctx.getCurrentKey, "order Pay success"))
          ctx.timerService().deleteEventTimeTimer(timerTs)
          isPayedState.clear()
          timerState.clear()
        } else {
          // 如果没有pay过等待pay 的到来
          val ts = value.eventTime * 1000 + 15 * 60 * 1000
          ctx.timerService().registerEventTimeTimer(ts)
          timerState.update(ts)
        }
      } else if (value.eventType == "pay") {
        // 如果是pay 事件，那么继续判断create 是否来了,可以用timer 标示
        if (timerTs > 0) {
          // 如果有定时器，说明已经有create 来过
          //继续判断是否超过了timeout 事件
          if (timerTs > value.eventTime * 1000L) {
            // 如果定时器事件还没到，那么输出成功匹配
            out.collect(OrderResult(value.orderId, "order Pay success"))
          } else {
            // 如果当前pay  的事件已经超时，那么输出到侧输出流
            ctx.output(orderTimeoutOutputTag, OrderResult(value.orderId, "payed but already timeout"))
          }
          // 输出结束，清空状态
          ctx.timerService().deleteEventTimeTimer(timerTs)
          isPayedState.clear()
          timerState.clear()
        } else {
          // pay 先到,更新状态，注册定时器等到create
          isPayedState.update(true)
          ctx.timerService().registerEventTimeTimer(value.eventTime * 1000l)
          timerState.update(value.eventTime * 1000)
        }
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {

      // 根据状态的值判断哪个数据没来，
      if (isPayedState.value()) {
        // 如果为true  标示pay  先到了，没等到create
        ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "already payed but not found create"))
      } else {
        // 标示create到了，没等到pay
        ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "order timeout"))
      }
      isPayedState.clear()
      timerState.clear()
    }

  }

}

class OrderTimeoutWarning() extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {
  //  保存支付是否来过的状态
  lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isPayed-state", classOf[Boolean]))

  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {

    // 先取出状态标识符
    val isPayed = isPayedState.value()

    // 如果遇到了create 事件，并且pay 没有来过注册定时器开始等待
    if (value.eventType == "create" && !isPayed) {
      ctx.timerService().registerEventTimeTimer(value.eventTime * 1000 + 15 * 60 * 1000) // 15分钟,仅处理一个create 一个pay 的情况
    } else if (value.eventType == "pay") {
      // 如果是pay 事件，直接把状态改为true
      isPayedState.update(true)
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {

    // 判断isPayed  是否为true
    val isPayed = isPayedState.value()
    if (isPayed) {
      out.collect(OrderResult(ctx.getCurrentKey, "order Pay success"))
    } else {
      out.collect(OrderResult(ctx.getCurrentKey, "order Pay timeout"))
    }
    // 清空状态
    isPayedState.clear()
  }
}

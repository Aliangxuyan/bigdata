package com.lxy.orderpay_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * @author lxy
  *         2020/2/13
  *
  *         订单超时未支付——状态编程
  *
  *         7、交易订单超过多久未支付（分析 CEP 底层实现，用状态编程，进一步的了解，这种比较复杂，不建议）
  *
  *         订单支付实时监控
  *         • 基本需求
  *         – 用户下单之后，应设置订单失效时间，以提高用户支付的意愿，并降
  *         低系统风险
  *         – 用户下单后15分钟未支付，则输出监控信息
  *
  *         • 解决思路
  *         – 利用 CEP 库进行事件流的模式匹配，并设定匹配的时间间隔
  *         – 也可以利用状态编程，用 process function 实现处理逻辑
  *
  *         传统的实现
  *         1）可以redis 实现，
  *         2）启动一个任务去实时的扫描更新订单状态为失效，以及通知等
  *         存在问题：数据量很大的时候，对数据库的压力很大
  *
  *         力求实时性可以使用flink 实现，
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

  /**
    * 不考虑先来pay 再来 create  并且事件超过 15 分钟的情况，因为已超时太久数据本身不适合做实时事件处理
    *
    * 下面针对一个create 一个pay 并且存在乱序的情况进行状态编程处理
    */
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
        // 1、如果是create 事件，接下来判断pay  是否来过，
        if (isPayed) {
          // 1.1 如果已经pay 过，匹配成功输出主流数据，清空状态
          out.collect(OrderResult(ctx.getCurrentKey, "order Pay success"))
          ctx.timerService().deleteEventTimeTimer(timerTs)
          isPayedState.clear()
          timerState.clear()
        } else {
          // 1.2 如果没有pay过等待pay 的到来
          val ts = value.eventTime * 1000 + 15 * 60 * 1000
          ctx.timerService().registerEventTimeTimer(ts)
          timerState.update(ts)
        }
      } else if (value.eventType == "pay") {
        // 2、如果是pay 事件，那么继续判断create 是否来了,可以用timer 标示
        if (timerTs > 0) {
          // 2.1 如果有定时器，说明已经有create 来过
          //继续判断是否超过了timeout 事件
          if (timerTs > value.eventTime * 1000L) {
            // 2.1.1 如果定时器事件还没到，那么输出成功匹配
            out.collect(OrderResult(value.orderId, "order Pay success"))
          } else {
            // 2.1.2 如果当前pay  的事件已经超时，那么输出到侧输出流
            ctx.output(orderTimeoutOutputTag, OrderResult(value.orderId, "payed but already timeout"))
          }
          // 上面已经存在create 和pay ，，表示输出结束，可以清空状态
          ctx.timerService().deleteEventTimeTimer(timerTs)
          isPayedState.clear()
          timerState.clear()
        } else {
          // 2.2 pay 先到,更新状态，注册定时器等到create,该地方处理乱序事件，直到pay 的定时器事件
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
        // 标示create到了，没等到pay，标准的超时
        ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "order timeout"))
      }
      // 定时器触发之后，状态需要清空
      isPayedState.clear()
      timerState.clear()
    }

  }

}

/**
  * 超时报警，前面做过keyBy 可以直接使用 KeyedProcessFunction
  *
  * 这种可以处理简单的一个create 一个pay 的情况，和线create 后pay 的情况
  *
  * 问题：
  * 不能做到来一条马上输出，复杂的逻辑处理不太好比如，先来pay 后来 create  的情况
  * 而且要等到定时器触发
  *
  * ,仅处理一个create 一个pay 的情况，
  */
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

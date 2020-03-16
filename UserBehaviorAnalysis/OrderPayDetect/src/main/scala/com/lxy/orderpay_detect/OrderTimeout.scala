package com.lxy.orderpay_detect

import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @author lxy
  *         2020/2/13
  *         交易订单超过多久未支付
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
  *         力求实时性可以使用flink 实现
  *
  *
  *
  *
  */

// 输入订单事件的样例类
case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)

// 输出结果的样例类
case class OrderResult(orderId: Long, resultMsg: String)


object OrderTimeout {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 1、读取订单数据
    val resource = getClass.getResource("/OrderLog.csv")
    //        val orderEventStream = env.readTextFile(resource.getPath)
    val orderEventStream = env.socketTextStream("localhost", 7777)
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.orderId) // 针对同一个订单所有需要keyBy()


    // 2、定义一个匹配模式
    val orderPayPattern = Pattern.begin[OrderEvent]("begin").where(_.eventType == "create")
      .followedBy("follow").where(_.eventType == "pay")
      .within(Time.minutes(15))

    // 3、把模式应用到stream  上得到一个pattern stream
    val patternStream = CEP.pattern(orderEventStream, orderPayPattern)

    // 4、调用select 方法提取时间序列，超时时间要做报警提示
    val orderTimeoutOutPutTag = new OutputTag[OrderResult]("orderTimeOut")

    // 如果直接是使用select ,返回的是在15 分钟内成功支付的事件，而现在需求是超时位支付的订单需要做报警处理
    // 将超时未支付的放到侧输出流，进行报警处理
    val resultStream = patternStream.select(orderTimeoutOutPutTag,
      new OrderTimeoutSelect(),
      new OrderPaySelect())

    resultStream.print("payed")
    resultStream.getSideOutput(orderTimeoutOutPutTag).print("timeout")
    env.execute("OrderTimeout pay job")

  }

  // 自定义超时事件序列处理函数
  class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult] {
    override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
      val timeoutOrderId = map.get("begin").iterator().next().orderId
      OrderResult(timeoutOrderId, "timeout")
    }
  }

  // 自定义正常支付事件处理函数
  class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult] {
    override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
      val payedOrderId = map.get("follow").iterator.next().orderId
      OrderResult(payedOrderId, "orderPay success!!!")
    }
  }

}

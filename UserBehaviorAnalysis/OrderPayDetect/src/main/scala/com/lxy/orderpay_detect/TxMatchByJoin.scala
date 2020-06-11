package com.lxy.orderpay_detect

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * @author lxy
  *         2020/2/13
  *
  *         8、 支付对账 —— intervalJoin
  *
  *         如果是两条流做join 后面开窗口的话，最简单的方式是做windowjoin,但是该需求不适合这样做
  *
  *         订单支付实时对账
  *         • 基本需求
  *         – 用户下单并支付后，应查询到账信息，进行实时对账
  *         – 如果有不匹配的支付信息或者到账信息，输出提示信息
  *
  *         • 解决思路
  *         – 从两条流中分别读取订单支付信息和到账信息，合并处理
  *         – 用 connect 连接合并两条流，用 coProcessFunction 做匹配处理
  *
  *
  *         和上面 TxMatchDetect 中的
  *         CoProcessFunction  的对比，更简单，但是存在缺陷：不能做侧输出流处理，正常数据可以输出，异常数据没有处理
  *
  *         但是：温度和烟雾报警两条流的情况特别适合用 CoProcessFunction，等同时温度和烟雾两条都满足时进行报警
  *
  *         总结：CoProcessFunction 比较适合做匹配事件，对于不匹配事件不适合，更适合用侧输出流的方式实现
  */

object TxMatchByJoin {
  // 定义侧输出流tag
  // 没有匹配到pay
  val unmatchedPays = new OutputTag[OrderEvent]("unmatchedPays")
  // 没有匹配搭配order
  val unmatchedReceipts = new OutputTag[ReceiptEvent]("unmatchedReceipts")

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
      .filter(_.txId != "")
      .assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.txId)


    // 1、读取支付到账数据流
    val receiptResource = getClass.getResource("/ReceiptLog.csv")
    val receiptEventStream = env.readTextFile(receiptResource.getPath)
      //    val receiptEventStream = env.socketTextStream("localhost", 8888)
      .map(data => {
        val dataArray = data.split(",")
        ReceiptEvent(dataArray(0).trim, dataArray(1).trim, dataArray(2).trim.toLong)
      })
      .filter(_.txId != "")
      .assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.txId)

    // join 处理
    val processedStream = orderEventStream.intervalJoin(receiptEventStream)
      .between(Time.seconds(-5), Time.seconds(5))
      .process(new TxPayMatchByJoin())

    processedStream.print()
    env.execute("TxMatchByJoin job")

  }

  // 和上面 CoProcessFunction  的对比，更简单，但是存在缺陷：不能做侧输出流处理，正常数据可以输出，异常数据没有处理
  /**
    * 温度和烟雾报警两条流的情况特别适合用 CoProcessFunction
    */
  class TxPayMatchByJoin() extends ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {
    override def processElement(left: OrderEvent, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      out.collect((left, right))
    }
  }

}

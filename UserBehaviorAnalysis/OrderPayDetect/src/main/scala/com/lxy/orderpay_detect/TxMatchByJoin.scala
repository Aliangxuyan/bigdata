package com.lxy.orderpay_detect

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * @author lxy
  *         2020/2/13
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
  class TxPayMatchByJoin() extends ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {
    override def processElement(left: OrderEvent, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      out.collect((left, right))
    }
  }

}

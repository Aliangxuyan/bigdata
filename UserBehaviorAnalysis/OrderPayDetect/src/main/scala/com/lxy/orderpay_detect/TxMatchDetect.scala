package com.lxy.orderpay_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * @author lxy
  *         2020/2/13
  *
  *         支付对账
  */

// 定义接受流数据事件的样例类
case class ReceiptEvent(txId: String, payChannel: String, eventTime: Long)

object TxMatchDetect {

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
    //    val orderEventStream = env.readTextFile(resource.getPath)
    val orderEventStream = env.socketTextStream("localhost", 7777)
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .filter(_.txId != "")
      .assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.txId)


    // 1、读取支付到账数据流
    //    val receiptResource = getClass.getResource("/ReceiptLog.csv")
    //    val receiptEventStream = env.readTextFile(receiptResource.getPath)
    val receiptEventStream = env.socketTextStream("localhost", 8888)
      .map(data => {
        val dataArray = data.split(",")
        ReceiptEvent(dataArray(0).trim, dataArray(1).trim, dataArray(2).trim.toLong)
      })
      .filter(_.txId != "")
      .assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.txId)

    // 定义输入流
    val processedStream = orderEventStream.connect(receiptEventStream)
      .process(new TxPayMatch())


    processedStream.getSideOutput(unmatchedPays).print("unmatchedPays")
    processedStream.getSideOutput(unmatchedReceipts).print("unmatchedReceipts")

    processedStream.print("matched")
    env.execute("tx match job")
  }

  class TxPayMatch() extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {

    // 定义状态来保存已经到达的订单支付事件和到账事件
    lazy val payState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay-state", classOf[OrderEvent]))
    lazy val receiptState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt-state", classOf[ReceiptEvent]))

    // 订单支付事件数据的处理
    override def processElement1(pay: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      // 判断有没有对应的到账事件
      val receipt = receiptState.value()
      if (receipt != null) {
        // 如果已经有receipt ，在主流输出匹配信息
        out.collect((pay, receipt))
        receiptState.clear()
      } else {
        // 如果还没到，那么把pay 存入状态，并且注册定时器等待
        payState.update(pay)
        ctx.timerService().registerEventTimeTimer(pay.eventTime * 1000 + 5 * 1000l)
      }
    }

    //到账事件的处理
    override def processElement2(receipt: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      // 同样的处理流程
      val pay = payState.value()
      if (pay != null) {
        out.collect((pay, receipt))
        payState.clear()
      } else {
        receiptState.update(receipt)
        ctx.timerService().registerEventTimeTimer(receipt.eventTime * 1000 + 5 * 1000l)
      }
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {

      // 事件到了，如果还没有收到某个事件，那么输出报警信息
      if (payState.value() != null) {
        // 证明receipt 没有来
        ctx.output(unmatchedPays, payState.value())
      }
      if (receiptState.value() != null) {
        // 证明pay 没有来
        ctx.output(unmatchedReceipts, receiptState.value())
      }
      payState.clear()
      receiptState.clear()
    }
  }

}

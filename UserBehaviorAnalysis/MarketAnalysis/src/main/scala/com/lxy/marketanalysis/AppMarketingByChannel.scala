package com.lxy.marketanalysis

import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
  * @author lxy
  *         2020-02-12
  *
  *         4、市场营销分析 —— APP 市场推广统计
  *         基本需求
  *         – 从埋点日志中，统计 APP 市场推广的数据指标
  *         – 按照不同的推广渠道，分别统计数据
  *         解决思路
  *         – 通过过滤日志中的用户行为，按照不同的渠道进行统计
  *         – 可以用 process function 处理，得到自定义的输出数据信息
  */
// 输入数据样例类
case class MarketingUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)

//输出结果样例类
case class MarketingViewCount(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)

object AppMarketingByChannel {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.addSource(new SimulatedEventSource())
      .assignAscendingTimestamps(_.timestamp) //毫秒
      .filter(_.behavior != "UNINSTALL")
      .map(data => {
        ((data.channel, data.behavior), 1L)
      })
      .keyBy(_._1) // 以渠道和行为类型做为key 分组
      .timeWindow(Time.hours(1), Time.seconds(10))
      .process(new MarketingCountByChannel())
    // 直接keyBy 之后的话，函数是 KeyedProcessFunction，次数是直接在开窗函数之后的所以函数类继承 ProcessWindowFunction

    dataStream.print()
    env.execute("AppMarketingByChannel job")
  }
}

// 自定义数据源
class SimulatedEventSource() extends RichSourceFunction[MarketingUserBehavior] {
  // 定义是否运行的标识位
  var running = true
  // 定义用户行为的集合
  val behaviorType: Seq[String] = Seq("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL")
  // 定义渠道的集合
  val channelSets: Seq[String] = Seq("wechat", "weibo", "appstore", "huaweistore")
  // 定义随机数发生器
  val rand: Random = new Random()

  override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    //定义一个生成数据的上线
    val maxElements = Long.MaxValue
    var count = 0L

    //随机生成所有数据
    while (running && count < maxElements) {
      val id = UUID.randomUUID().toString
      val behavior = behaviorType(rand.nextInt(behaviorType.size))
      val channel = channelSets(rand.nextInt(channelSets.size))
      val timestamp = System.currentTimeMillis()

      ctx.collect(MarketingUserBehavior(id, behavior, channel, timestamp))
      count += 1

      TimeUnit.MILLISECONDS.sleep(10L)
    }
  }

  override def cancel(): Unit = running = false
}

// 自定义处理函数
class MarketingCountByChannel() extends ProcessWindowFunction[((String, String), Long), MarketingViewCount, (String, String), TimeWindow] {
  override def process(key: (String, String), context: Context, elements: Iterable[((String, String), Long)], out: Collector[MarketingViewCount]): Unit = {
    val startTs = new Timestamp(context.window.getStart).toString
    val entTs = new Timestamp(context.window.getEnd).toString
    val channel = key._1
    val behavior = key._2
    val count = elements.size.toLong
    out.collect(MarketingViewCount(startTs, entTs, channel, behavior, count))
  }
}

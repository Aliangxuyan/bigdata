package com.lxy.marketanalysis

import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
  * @author lxy
  *         2020/2/12
  */

object AppMaketing {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.addSource(new SimulatedEventSource())
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior != "UNINSTALL")
      .map(data => {
        ("dunmyKey", 1L)
      })
      .keyBy(_._1) // 以渠道和行为类型做为key 分组
      .timeWindow(Time.hours(1), Time.seconds(10))
      .aggregate(new CountAgg(), new MarketingCountTotal()) // 增量聚合

    dataStream.print()
    env.execute("AppMarketingByChannel job")
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

  class CountAgg() extends AggregateFunction[(String, Long), Long, Long] {
    override def createAccumulator(): Long = 0L

    override def merge(a: Long, b: Long): Long = a + b

    override def getResult(acc: Long): Long = acc

    override def add(in: (String, Long), acc: Long): Long = acc + 1
  }

  class MarketingCountTotal() extends WindowFunction[Long, MarketingViewCount, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[MarketingViewCount]): Unit = {
      val startTs = new Timestamp(window.getStart).toString
      val entTs = new Timestamp(window.getEnd).toString
      val count = input.iterator.next()
      out.collect(MarketingViewCount(startTs, entTs, "app marketging", "total", count))
    }
  }

}
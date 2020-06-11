package com.lxy.networkflow_analysis

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * @author lxy
  * @date 2020-02-11
  *
  *       2、实时流量统计 —— 热门页面
  *
  *       注意：流量统计不能直接使用埋点日志，不过可以使用apache log
  *
  *       基本需求
  *       – 从web服务器的日志中，统计实时的热门访问页面
  *       – 统计每分钟的ip访问量，取出访问量最大的5个地址，每5秒更新一次
  *
  *       解决思路
  *       – 将 apache 服务器日志中的时间，转换为时间戳，作为 Event Time
  *       – 构建滑动窗口，窗口长度为1分钟，滑动距离为5秒
  *
  *
  *
  *
  */

// 输入数据样例类，时间使用时间戳
case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

// 窗口聚合结果样例类
case class UrlViewCount(url: String, windowEnd: Long, count: Long)

object NetWorkFlow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.readTextFile("/Users/lxy/Documents/Idea_workspace/bigdata/UserBehaviorAnalysis/NetWorkFlowAnalysis/src/main/resources/apache.log")
      .map(data => {
        val dataArray = data.split(" ")
        // 定义时间转换
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp = simpleDateFormat.parse(dataArray(3).trim).getTime
        ApacheLogEvent(dataArray(0).trim, dataArray(1).trim, timestamp, dataArray(5).trim, dataArray(6).trim)
      })
      // 乱序的话不能直接使用 assignAscendingTimestamps 提取时间戳方法，此处使用的是 周期性watermark   BoundedOutOfOrdernessTimestampExtractor
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
      override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
    })
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      // 允许60s 的迟到数据处理延迟到的数据，实际生产中不会有特别大延迟的数据，如果遇到延迟特别大的数据可以考虑sideout 侧输出流
      .allowedLateness(Time.seconds(60))
      .aggregate(new CountAgg(), new WindowResult())
      .keyBy(_.windowEnd)
      .process(new TopNHotUrl(5))

    dataStream.print()

    env.execute("newwork flow job")

  }

  // 自定义预聚合函数
  class CountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
    override def createAccumulator(): Long = 0

    override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(a: Long, b: Long): Long = a + b
  }

  // 自定义窗口处理函数
  class WindowResult() extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {

    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
      out.collect(UrlViewCount(key, window.getEnd, input.iterator.next()))
    }
  }

  // 自定义排序输出处理函数
  class TopNHotUrl(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {

    // **** 状态可以在open（）方法中定义也可以使用lazy 定义
    lazy val urlState: ListState[UrlViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("url-state", classOf[UrlViewCount]))

    override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
      urlState.add(value)
      // 定时器触发的时间可以在windowend 基础上自己设置
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      // 从状态中拿到数据
      val allUrlViews: ListBuffer[UrlViewCount] = new ListBuffer[UrlViewCount]
      val iter = urlState.get().iterator()

      // 想使用foreach  必须有下面引入，或者可以使用迭代器便利
      //      import scala.collection.JavaConversions._
      //      for (item <- urlState.get()) {
      //        allUrlViews += item
      //      }

      while (iter.hasNext) {
        allUrlViews += iter.next()
      }
      urlState.clear()

      // 或者可以使用 sortby() 方法排序
      val sortedUrlViews = allUrlViews.sortWith(_.count > _.count).take(topSize)

      // 格式化结果输出
      val result: StringBuilder = new StringBuilder
      result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")

      for (i <- sortedUrlViews.indices) {
        val currentUrlView = sortedUrlViews(i)
        result.append("NO ").append(i + 1).append(":")
          .append("URL=").append(currentUrlView.url)
          .append("访问量=").append(currentUrlView.count).append("\n")
      }

      result.append("*****************************************")
      Thread.sleep(1000)
      out.collect(result.toString())
    }
  }

}

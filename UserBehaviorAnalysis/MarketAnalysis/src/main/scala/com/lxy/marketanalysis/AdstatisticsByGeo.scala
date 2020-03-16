package com.lxy.marketanalysis

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * @author lxy
  *         2020/2/12
  *
  *         • 基本需求
  *
  *         – 从埋点日志中，统计每小时页面广告的点击量，5秒刷新一次，并按照不同省
  *         份进行划分
  *         – 对于“刷单”式的频繁点击行为进行过滤，并将该用户加入黑名单
  *
  *         • 解决思路
  *         – 根据省份进行分组，创建长度为1小时、滑动距离为5秒的时间窗口进行统计
  *         – 可以用 process function 进行黑名单过滤，检测用户对同一广告的点击量， 如果超过上限则将用户信息以侧输出流输出到黑名单中
  *
  */

// 输入的广告点击事件样例类
case class AdClickEvent(userID: Long, adId: Long, province: String, city: String, timestamp: Long)

// 按照省份统计的输出结果样例类
case class CountByProvince(windowEnd: String, province: String, count: Long)

//输出的黑名单的报警信息
case class BlackListWarning(userId: Long, adId: Long, msg: String)


// 读取数据并转换为AdClickEvent
object AdstatisticsByGeo {

  // 定义侧输出流的标签
  val blackListOutPutTag: OutputTag[BlackListWarning] = new OutputTag[BlackListWarning]("blankList")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/AdClickLog.csv")
    val adEventStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        AdClickEvent(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toString, dataArray(3).trim.toString, dataArray(4).trim.toLong)
      }).assignAscendingTimestamps(_.timestamp * 1000L)


    // 涉及到状态编程和定时器直接自定义process function 过滤掉大量刷点击的行为
    val filterBlackListStream = adEventStream
      .keyBy(data => (data.userID, data.adId))
      .process(new FilterBlackListUser(100))

    // 根据省份做分组，开窗聚合
    val adCountStream = filterBlackListStream
      .keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new AdCountAgg(), new AdCountResult())


    adCountStream.print("count")
    filterBlackListStream.getSideOutput(blackListOutPutTag).print("blankList")

    env.execute("ad statistics job")
  }

  class FilterBlackListUser(maxCount: Int) extends KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent] {

    // 定义状态，保存当前用户对当前广告的点击量
    lazy val countState: ValueState[(Long)] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-state", classOf[Long]))

    // 保存是否发送过黑名单的状态
    lazy val isSentBlackList: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("issent-state", classOf[Boolean]))

    // 保存定时器触发的时间戳
    lazy val resetTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("resettime-state", classOf[Long]))

    //
    override def processElement(value: AdClickEvent, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context, out: Collector[AdClickEvent]): Unit = {
      // 取出count 状态
      val curCount = countState.value()

      // 如果是第一次处理，注册定时器,每天00：00 触发，注册一次就可以
      if (curCount == 0) {
        // 当前时间的第二天，即明天
        val ts = (ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * (1000 * 60 * 60 * 24)
        resetTimer.update(ts)
        ctx.timerService().registerProcessingTimeTimer(ts)
      }

      // 判断计数是否达到上限，如果达到则加入黑名单
      if (curCount >= maxCount) {
        // 判断是否发送过黑名单，只发送一次
        if (!isSentBlackList.value()) {
          isSentBlackList.update(true)
          // 输出到侧输出流
          ctx.output(blackListOutPutTag, BlackListWarning(value.userID, value.adId, "Click over " + maxCount + "times today"))
        }
        return
      }

      // 计数状态加 1 ，输出数据到主流
      countState.update(curCount + 1)

      out.collect(value)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
      // 定时器触发时，清空状态
      if (timestamp == resetTimer.value()) {
        isSentBlackList.clear()
        countState.clear()
        resetTimer.clear()
      }
    }
  }

  class AdCountAgg() extends AggregateFunction[AdClickEvent, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def merge(a: Long, b: Long): Long = a + b

    override def getResult(acc: Long): Long = acc

    override def add(in: AdClickEvent, acc: Long): Long = acc + 1
  }

  class AdCountResult() extends WindowFunction[Long, CountByProvince, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
      out.collect(CountByProvince((new Timestamp(window.getEnd)).toString, key, input.iterator.next()))
    }
  }

}



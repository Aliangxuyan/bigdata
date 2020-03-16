package com.lxy.networkflow_analysis

import com.lxy.networkflow_analysis.PageView.getClass
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * @author lxy
  * @date 2020-02-11
  *
  *       3、实时流量统计 —— PV 和 UV
  *       uv：独立访客数
  *
  *       基本需求
  *       – 从埋点日志中，统计实时的 PV 和 UV
  *       – 统计每小时的访问量(PV)，并且对用户进行去重(UV)
  *       解决思路
  *       – 统计埋点日志中的 pv 行为，利用 Set 数据结构进行去重
  *       – 对于超大规模的数据，可以考虑用布隆过滤器进行去重
  *
  *
  *       存在问题：去重操作是将userID 放到set 中进行去重，数据量更大的话，该实现不合理，redis 也存在同样的问题，
  *
  *
  *       方案：因为该需求不关心key 是什么，只是关心有没有，可以用 0 | 1 表示，进而可以用位图，思路，把所有的状态都保存到位图
  *
  *       布隆过滤器实现（底层就是位图）：
  *
  *       后续：
  *       下面实现是将所有数据到达之后进行的处理，还可以来一条处理一条
  *
  *
  *
  */

case class UvCount(windowEnd: Long, uvCount: Long)

object UniqueVisitor {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 用相对路径定义数据源
    val resource = getClass.getResource("/UserBehavior.csv")
    val dataStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.behavior == "pv") // 只统计PV 操作
      .timeWindowAll(Time.hours(1)) // 针对时间窗口去重
      .apply(new UvCountByWindow()) // 此处可以是直接使用与聚合函数，也可以是先收集数据给windowfunction

    dataStream.print("pv count")
    env.execute("unique  job")
  }

  class UvCountByWindow() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow] {
    override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
      // 定义一个scala set，用于保存所有的数据userId并去重
      var idSet = Set[Long]()
      // 把当前窗口所有数据的ID收集到set中，最后输出set的大小
      for (userBehavior <- input) {
        idSet += userBehavior.userId
      }
      out.collect(UvCount(window.getEnd, idSet.size))
    }
  }

}

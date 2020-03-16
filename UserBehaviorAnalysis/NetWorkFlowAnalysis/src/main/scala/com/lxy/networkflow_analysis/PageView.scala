package com.lxy.networkflow_analysis


import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @author lxy
  * @date 2020-02-11
  *
  *
  *       3、实时流量统计 —— PV 和 UV
  *
  *       基本需求
  *       – 从埋点日志中，统计实时的 PV 和 UV
  *       – 统计每小时的访问量(PV)，并且对用户进行去重(UV)
  *
  *       解决思路
  *       – 统计埋点日志中的 pv 行为，利用 Set 数据结构进行去重
  *       – 对于超大规模的数据，可以考虑用布隆过滤器进行去重
  *
  *
  *       可以是实时大屏显示，统计近1分钟的总订单趋势以及不同类型订单【现制和预估值】
  *       （餐厅公众号【未支付，已回调，退单成功，回到成功，回调失败】【未支付，已支付，已完成】，
  *         餐厅小程序，商城，
  *         外卖(美团，饿了么，自营外卖等)【已消费，待消费】，openAPI，POS 主被扫）
  *         订单营收
  *
  */

// 定义输入数据的样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

object PageView {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 用相对路径定义数据源
    /**
      * 相比 val dataStream = env.readTextFile(
      *
      *  getClass.getResource
      *  env.readTextFile
      * 这种相对路径开发环境更加常见些
      */
    val resource = getClass.getResource("/UserBehavior.csv")
    val dataStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      // 分类时间戳因为有序，直接使用 assignAscendingTimestamps 就可以
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.behavior == "pv") // 只统计PV 操作
      .map(data => ("pv", 1))
      .keyBy(_._1) // 方便使用时间窗口，使用keyBy() 其实这块没有任何的作用
      .timeWindow(Time.hours(1))
      .sum(1)

    dataStream.print("pv count")
    env.execute("page view job")
  }
}

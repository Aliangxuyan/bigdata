package com.lxy.networkflow_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
  * @author lxy
  * @date 2020-02-11
  *
  *
  *       3、实时流量统计 —— PV 和 UV
  *
  *       uv：独立访客数
  *       基本需求
  *       – 从埋点日志中，统计实时的 PV 和 UV
  *       – 统计每小时的访问量(PV)，并且对用户进行去重(UV)
  *       解决思路
  *       – 统计埋点日志中的 pv 行为，利用 Set 数据结构进行去重
  *       – 对于超大规模的数据，可以考虑用布隆过滤器进行去重
  *
  *       方案：因为该需求不关心key 是什么，只是关心有没有，可以用 0 | 1 表示，进而可以用位图，思路，把所有的状态都保存到位图
  *       布隆过滤器实现（底层就是位图）：
  *
  *       后续：
  *       下面实现是将所有数据到达之后进行的处理，还可以来一条处理一条
  *
  *
  *       存在问题：
  *       现在是来一条数据统计输出一次，都要做读写redis 操作，时间换空间啦，
  *       可以考虑窗口分片，将窗口的信息可以线存到内存
  *
  */
object UvWithbBloom {
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
      .map(data => ("dummyKey", data.userId))
      .keyBy(_._1) // key 是第一个元素 dummyKey ，所以相当于所有的数据都来啦
      .timeWindow(Time.hours(1))
      .trigger(new MyTtigger()) // 触发器，之前是都将数据加载到内存里面再做操作，现在不能让写入内存，数据量会撑爆内存，所以使用触发器
      .process(new UvCountWithBloom())

    // 窗口处理函数是在当前数据收集到啦，等要关闭的时候猜调用

    dataStream.print()

    env.execute("uv with bloom job ")

  }

  // 自定义窗口触发器
  /**
    * 默认是 EventTime 的时候触发，前面定一个 EventTime 时间语义 EventTimeTrigger，
    * 看源码：是每来一个元素判断下，窗口的最大时间戳如果大于等于 CurrentWatermark ，返回return TriggerResult.FIRE; 表示触发
    *
    * getCurrentWater mark
    * EventTimeTrigger：
    * public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
    * if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
    * // if the watermark is already past the window fire immediately
    * return TriggerResult.FIRE;
    * } else {
    * 			ctx.registerEventTimeTimer(window.maxTimestamp());
    * return TriggerResult.CONTINUE;
    * }
    * }
    *
    *
    */
  class MyTtigger() extends Trigger[(String, Long), TimeWindow] {

    // 每来一条数据就直接触发窗口操作，并清空所有窗口状态
    override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE

    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = ()
  }

  // 定义一个布隆过滤器
  class Bloom(size: Long) extends Serializable {
    // 位图的总大小 1 << 27    1<<4  16bit  1<<4 << 20  16M     1<<4 << 20 << 3  到字节
    // 可以存储 16M 大小的数据
    private val cap = if (size > 0) size else 1 << 27

    // 定义哈希函数
    def hash(value: String, seed: Int): Long = {
      var result: Long = 0L

      // 根据位每一个字符的编码进行叠加计算
      for (i <- 0 until value.length) {
        result = result * seed + value.charAt(i)
      }
      result & (cap - 1) // 直接result  返回的是长整型，和 cap-1  是27 个1 ，和 （ cap-1） 做与运算肯定得到的数据再0到27个1 之间
    }
  }


  class UvCountWithBloom() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] {

    // 定义redis  链接，只需要定义一次就可以，所有使用lazy
    lazy val jedis = new Jedis("localhost", 6379)

    // https://blog.csdn.net/u010003835/article/details/84940750  redis  一个字符串类型的值最多能存储512M字节的内容。  1<<29
    lazy val bloom = new Bloom(1 << 29) // 64M 大小的布隆过滤器 1<<27 是 16M,能够处理5亿多个Key

    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
      //位图的存储方式，key 是windowEnd value 是 bitMap,每个窗口一个位图，互不影响
      val storeKey = context.window.getEnd.toString
      var count = 0L

      // 把每个窗口的 uv count 值存入名为count 对存储在redis表存放内容为（windowEnd,uvCount） ，所以要先从redis 中读取
      if (jedis.hget("count", storeKey) != null) {
        count = jedis.hget("count", storeKey).toLong
      }

      // 判断用布隆过滤器判断当前用户是否已经存在
      val userId = elements.last._2.toString
      val offset = bloom.hash(userId, 61) // 一般分配一个质数，尽可能分配散列

      // 定义一个标志位，判断redis 位图中有没有这一位
      val isExist = jedis.getbit(storeKey, offset)

      if (!isExist) {
        // 如果不存在，位图对应位置1 ，count + 1
        jedis.setbit(storeKey, offset, true)

        jedis.hset("count", storeKey, (count + 1).toString)

        out.collect(UvCount(storeKey.toLong, count + 1))
      } else {
        out.collect(UvCount(storeKey.toLong, count))
      }
    }
  }

}

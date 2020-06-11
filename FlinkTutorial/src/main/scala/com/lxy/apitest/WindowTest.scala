package com.lxy.apitest

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * @author lxy
  * @date 2020-02-05
  *
  *       wateremark 水位线
  *
  *       1、时间语义
  *       1）TimeCharacteristic.EventTime （事件创建时间）
  *       2）TimeCharacteristic.IngestionTime （进入flink的事件）
  *       3）TimeCharacteristic.ProcessingTime （进入操作系统的本地时间，和机器相关）
  *
  *       2、TimeCharacteristic.EventTime(实际中应用比较多，更具有意义)
  *       1）对执行环境设置流的时间特性：env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  *       2）从数据中提取时间戳
  *
  *
  *
  *       EventTime 时间特性使用，
  *       1）一定要指定数据源中的时间戳
  *       2）对于排好序的时间，只需要执行时间戳就可以，不需要延迟触发 .assignAscendingTimestamps(_.timestamp * 1000L)
  *
  *       3）乱序数据：
  *       1）.assignTimestampsAndWatermarks（TimestampAssigner）
  *       2）flink 提供了  TimestampAssigner 接口，以供我们实现，我们可以自定义如何获取时间戳和生成水位线
  *
  *
  *       生成watermark 的两种方式：
  *       1）AssignerWithPeriodicWatermarks （周期性生成watermark）系统会周期性的将watermark插入到数据流中
  *       abstract class BoundedOutOfOrdernessTimestampExtractor<T> implements AssignerWithPeriodicWatermark 也是周期性的生成水位线，默认是200ms
  *
  *
  *       2）AssignerWithPunctuatedWatermarks(没有时间周期规律，可以打断性的生成watermark)
  *
  *
  *
  *
  */
object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 设置事件时间特性，不设置的话默认是事件处理事件
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(1000)

    // 读入数据
            val inputStream = env.readTextFile("/Users/lxy/Documents/Idea_workspace/bigdata/FlinkTutorial/src/main/resources/sensor.txt")

//    val inputStream = env.socketTextStream("localhost", 7777)

    val dataStream = inputStream
      .map(
        data => {
          val dataArray = data.split(",")
          SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
        }
      )
      //      .assignAscendingTimestamps(_.timestamp * 1000L)  处理顺序事件
      //      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      //      override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      //    }) // 处理乱序事件
      .assignTimestampsAndWatermarks(new MyAssigner())
      .map(data => (data.id, data.temperature))
      .keyBy(_._1)
      //        .process( new MyProcess() )
      .timeWindow(Time.seconds(10), Time.seconds(3))
      .reduce((result, data) => (data._1,result._2.min(data._2))) // 统计10秒内的最低温度值


    dataStream.print()

    env.execute("window api test")
  }
}

/**
  * 间断性的水位线
  */
class MyAssigner() extends AssignerWithPeriodicWatermarks[SensorReading] {
  // 定义固定延迟为3秒
  val bound: Long = 3 * 1000L
  // 定义当前收到的最大的时间戳
  var maxTs: Long = Long.MinValue

  var watermark: Watermark = null

  //生成watermark
  override def getCurrentWatermark: Watermark = {
//    println("*********************watermark:" + (maxTs - bound))
    watermark = new Watermark(maxTs - bound)
    watermark
  }

  //抽取timestamp 数据本身的Event time来获取
  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    maxTs = maxTs.max(element.timestamp * 1000L)
    println("&&&&&&&&&&&&&&&&&&&&eventTime:" + element.timestamp * 1000 + "&&&&&&& water:" + watermark + "$$$$$$$$$$ maxTs:" + maxTs + "sesor: " + element)
    element.timestamp * 1000L
  }
}

/**
  * 不间断水位线
  *
  *
  */
class MyAssigner2() extends AssignerWithPunctuatedWatermarks[SensorReading] {
  val bound: Long = 1000L

  override def checkAndGetNextWatermark(lastElement: SensorReading, extractedTimestamp: Long): Watermark = {
    if (lastElement.id == "sensor_1") {
      new Watermark(extractedTimestamp - bound)
    } else {
      null
    }
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    element.timestamp * 1000L
  }
}

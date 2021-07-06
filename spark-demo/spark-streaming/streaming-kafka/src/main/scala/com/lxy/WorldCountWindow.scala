package com.lxy

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author lxy
 * @date 2021/7/6
 *
 *       3 秒一个批次，窗口 12 秒，滑步 6 秒。
 *       窗口时长：计算内容的时间范围
 *       滑动步长：隔多久触发一次计算
 *       这两者都必须为采集周期大小的整数倍,
 */
object WorldCountWindow {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("./ck")

    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("linux1", 9999)

    // Split each line into words
    val words = lines.flatMap(_.split(" "))

    // Count each word in each batch
    val pairs = words.map(word => (word, 1))

    // reduceByKeyAndWindow ：当窗口范围比较大，d但是滑动幅度比较小，可以采用增加或者删除的操作方式，无需重复计算，提升计算效率
    val wordCounts = pairs.reduceByKeyAndWindow((a: Int, b: Int) => (a + b), Seconds(12), Seconds(6))

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }

}

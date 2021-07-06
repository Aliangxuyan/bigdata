package com.lxy

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._

/**
 * @author lxy
 * @date 2021/7/6
 *       有状态转化操作
 */
object WorldCount {

  def main(args: Array[String]) {

    // 定义更新状态方法，参数 values 为当前批次单词频度，state 为以往批次单词频度
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }

    val conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(3))

    //使用有状态操作时需要设定检查点路径
    ssc.checkpoint("./ck")

    // Create a DStream that will connect to hostname:port, like hadoop102:9999
    val lines = ssc.socketTextStream("127.0.0.1", 9999)

    // Split each line into words
    val words = lines.flatMap(_.split(" "))


    //not necessary since Spark 1.3
    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
//
//    val wordCount = pairs.reduceByKey(_ + _)
//    wordCount.print()

    // 使用 updateStateByKey 来更新状态，统计从运行开始以来单词总的次数
    val stateDstream = pairs.updateStateByKey[Int](updateFunc)
    stateDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

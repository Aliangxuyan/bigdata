package com.lxy

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
 * @author lxy
 * @date 2021/7/6
 */
object Transform {

  def main(args: Array[String]): Unit = {

    //创建 SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建 StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //创建 DStream
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("linux1", 9999)

    //转换为 RDD 操作
    val wordAndCountDStream: DStream[(String, Int)] = lineDStream.transform(rdd => {

      val words: RDD[String] = rdd.flatMap(_.split(" "))
      val wordAndOne: RDD[(String, Int)] = words.map((_, 1))
      val value: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)

      value
    })

    //打印
    wordAndCountDStream.print

    //启动
    ssc.start()
    ssc.awaitTermination()

  }


}

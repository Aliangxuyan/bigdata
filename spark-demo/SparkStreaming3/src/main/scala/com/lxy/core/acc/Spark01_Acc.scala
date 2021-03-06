package com.lxy.core.acc

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author lxy
 * @date 2021/7/7
 *
 *
 */
object Spark01_Acc {
  def main(args: Array[String]): Unit = {

    val sparConf = new SparkConf().setMaster("local").setAppName("Acc")
    val sc = new SparkContext(sparConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    // reduce : 分区内计算，分区间计算
    //val i: Int = rdd.reduce(_+_)
    //println(i)
    var sum = 0
    //    rdd.collect().foreach(
    //      num => {
    //        sum = sum + num
    //      }
    //    )
    //
    rdd.foreach(
      num => {
        sum += num
      }
    )
    println("sum = " + sum)

    sc.stop()

  }
}

package com.lxy.core.rdd.io

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author lxy
 * @date 2021/7/7
 *
 * saveAsTextFile 和 saveAsObjectFile 对RDD 没有要求
 * 但是 saveAsSequenceFile 需要时K_V 类型RDD
 * objectFile
 *
 *
 */
object Spark01_RDD_IO_save {
  def main(args: Array[String]): Unit = {

    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)
    val rdd = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))


    rdd.saveAsTextFile("output1")
    rdd.saveAsObjectFile("output2")
    rdd.saveAsSequenceFile("output3")

    sc.stop()
  }
}

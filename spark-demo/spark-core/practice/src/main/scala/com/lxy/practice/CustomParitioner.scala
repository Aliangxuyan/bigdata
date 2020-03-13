package com.lxy.practice

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * @author lxy
  *  2019-12-07
  */
class CustomParitioner(numPer: Int) extends Partitioner {
  override def numPartitions: Int = numPer

  override def getPartition(key: Any): Int = {
    val ckey = key.toString
    ckey.substring(ckey.length - 1).toInt % numPer
  }
}

object Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("partitioner").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val data = sc.makeRDD(List("aa.2", "cc.2", "ff.2", "dd.3")).map((_, 1))

    val result = data.partitionBy(new CustomParitioner(8))

    result.mapPartitionsWithIndex((index,items) => Iterator(index + ":" + items.mkString("|"))).collect()

    sc.stop()
  }
}

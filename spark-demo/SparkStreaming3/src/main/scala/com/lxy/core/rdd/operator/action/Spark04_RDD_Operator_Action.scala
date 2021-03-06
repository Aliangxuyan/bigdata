package com.lxy.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author lxy
 * @date 2021/7/7
 *       countByValue：可以统计某个值出现的次数，非K-V 类型
 *       countByKey：K-V 类型
 */
object Spark04_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //val rdd = sc.makeRDD(List(1,1,1,4),2)
    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3)
    ))

    // TODO - 行动算子

    //val intToLong: collection.Map[Int, Long] = rdd.countByValue()
    //println(intToLong)
    val stringToLong: collection.Map[String, Long] = rdd.countByKey()
    println(stringToLong)

    sc.stop()

  }
}

package com.lxy.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author lxy
 * @date 2021/7/7
 */
object Spark04_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - mapPartitions
    val rdd: RDD[List[Int]] = sc.makeRDD(List(
      List(1, 2), List(3, 4)
    ))
    //    第一个list 为元素，第二个list  是分装的list 返回结果
    val flatRDD: RDD[Int] = rdd.flatMap(
      list => {
        list
      }
    )
    flatRDD.collect().foreach(println)


    sc.stop()

  }
}

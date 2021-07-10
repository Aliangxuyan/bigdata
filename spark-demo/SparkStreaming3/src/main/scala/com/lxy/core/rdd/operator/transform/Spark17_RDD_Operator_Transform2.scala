package com.lxy.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
/**
 * @author lxy
 * @date 2021/7/7
 *
 *
 * aggregateByKey:分区内分区件计算规则可以一样，初始值默认是0 时和 reduceByKey 没有区别
 *
 * foldByKey: 简化aggregateByKey 分区内和分区间计算规则一致的情况
 */
object Spark17_RDD_Operator_Transform2 {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - (Key - Value类型)

    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
      ("b", 4), ("b", 5), ("a", 6)
    ), 2)

    //rdd.aggregateByKey(0)(_+_, _+_).collect.foreach(println)

    // 如果聚合计算时，分区内和分区间计算规则相同，spark提供了简化的方法
    rdd.foldByKey(0)(_ + _).collect.foreach(println)


    sc.stop()

  }
}

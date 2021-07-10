package com.lxy.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author lxy
 * @date 2021/7/7
 * 算子： mapPartitionsWithIndex
 */
object Spark03_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - mapPartitionsWithIndex 增加了分区号即分区索引，分区编号从0开始，获取第二个分区的数据
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)
    // 【1，2】，【3，4】
    val mpiRDD = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        if (index == 1) {
          iter
        } else {
          Nil.iterator
        }
      }
    )
    mpiRDD.collect().foreach(println)
    sc.stop()

  }
}

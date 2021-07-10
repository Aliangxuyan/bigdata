package com.lxy.core.acc

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author lxy
 * @date 2021/7/7
 *
 *       longAccumulator 累加器
 *
 *       5.2.2.1系统累加器:
 *       sc.doubleAccumulator
 *       sc.collectionAccumulator
 *       sc.longAccumulator
 *
 */
object Spark02_Acc {
  def main(args: Array[String]): Unit = {

    val sparConf = new SparkConf().setMaster("local").setAppName("Acc")
    val sc = new SparkContext(sparConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    // 获取系统累加器
    // Spark默认就提供了简单数据聚合的累加器
    val sumAcc = sc.longAccumulator("sum")

    //sc.doubleAccumulator
    //sc.collectionAccumulator

    // 行动算子
    rdd.foreach(
      num => {
        // 使用累加器
        sumAcc.add(num)
      }
    )

    // 获取累加器的值
    println(sumAcc.value)

    sc.stop()

  }
}

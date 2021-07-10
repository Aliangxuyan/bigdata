package com.lxy.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @author lxy
 * @date 2021/7/7
 *
 *       partitionBy: 必须是 K-V 健值对类型RDD
 */
object Spark14_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - (Key - Value类型)
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)
    val mapRDD: RDD[(Int, Int)] = rdd.map((_, 1))
    // RDD => PairRDDFunctions
    // 隐式转换（二次编译）

    // partitionBy根据指定的分区规则对数据进行重分区
    //    如果重分区的分区器和当前RDD 的分区器一样,底层代码发现不会变化
    val newRDD = mapRDD.partitionBy(new HashPartitioner(2))
    newRDD.partitionBy(new HashPartitioner(2))

    newRDD.saveAsTextFile("output")


    sc.stop()

  }
}

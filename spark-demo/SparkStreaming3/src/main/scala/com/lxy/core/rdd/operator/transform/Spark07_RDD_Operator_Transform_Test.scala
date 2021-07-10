package com.lxy.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author lxy
 * @date 2021/7/7
 *
 * 从服务器日志数据 apache.log 中获取 2015 年 5 月 17 日的请求路径
 */
object Spark07_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - filter
    val rdd = sc.textFile("datas/apache.log")

    rdd.filter(
      line => {
        val datas = line.split(" ")
        val time = datas(3)
        time.startsWith("17/05/2015")
      }
    ).collect().foreach(println)


    sc.stop()

  }
}

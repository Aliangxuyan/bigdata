package com.lxy.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author lxy
 * @date 2021/7/7
 *
 *       算子：sample：根据指定的规则从数据集中抽取数据
 *
 *       使用场景：
 *       数据倾斜情况下，可以使用 sample 抽取多次，判断哪个key 次数最多，对key 多的数据进行特殊处理，防止数据倾斜
 *
 */
object Spark08_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - filter
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    // sample算子需要传递三个参数
    // 1. 第一个参数表示，抽取数据后是否将数据返回 true（放回），false（丢弃）
    // 2. 第二个参数表示，
    //         如果抽取不放回的场合：数据源中每条数据被抽取的概率，基准值的概念
    //         如果抽取放回的场合：表示数据源中的每条数据被抽取的可能次数
    // 3. 第三个参数表示，抽取数据时随机算法的种子
    //                    如果不传递第三个参数，那么使用的是当前系统时间，每次执行的数据是随机的，传值之后，种子固定，数据每次都不会变
    //        println(rdd.sample(
    //            false,
    //            0.4
    //            //1
    //        ).collect().mkString(","))

    println(rdd.sample(
      true,
      2
      //1
    ).collect().mkString(","))


    sc.stop()

  }
}

package com.lxy.practice

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author lxy
  * @date 2019-12-07
  *
  *       累加器的使用：
  *
  *       注意：累加器变量只能在driver  端读取，executor 端只能修改不能读取
  *       累加器最好不要用在转换操作中（可能被执行多次），而是用在行动操作中
  */
object Accu {

  def main(args: Array[String]): Unit = {
    // 创建sparkCOnf 对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("Practice")

    // 创建spackContext 对象
    val sc = new SparkContext(conf)
    //    var sum = 0  结果还是0 因为sum 控制权分别在 driver ，而在 executor中有执行所以得使用累加器解决类似问题
    //    // 申明累加器变量
    var sum = sc.accumulator(0)
    var rdd = sc.makeRDD(Array(1, 2, 3, 4, 5, 6, 7))

    rdd.map { x =>
      sum += x
    }.collect()

    println(sum.value)
    sc.stop()
  }
}

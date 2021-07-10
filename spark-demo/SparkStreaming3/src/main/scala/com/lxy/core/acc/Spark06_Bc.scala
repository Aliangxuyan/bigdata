package com.lxy.core.acc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author lxy
 * @date 2021/7/7
 *
 *       spark 的广播变量：broadcast
 *       * Excutor 其实就是一个JVM ，所以在启动的时候，会自动分配内存，完全可以将任务重的闭包数据放到EXcutor 的内存中
 *       * 达到共享的目的；
 *       spark 中的广播变量可以实现将闭包数据放到 Excutor的内存中了，不能够进行更改（分布式共享的只读变量）
 */
object Spark06_Bc {
  def main(args: Array[String]): Unit = {

    val sparConf = new SparkConf().setMaster("local").setAppName("Acc")
    val sc = new SparkContext(sparConf)

    val rdd1 = sc.makeRDD(List(
      ("a", 1), ("b", 2), ("c", 3)
    ))
    val map = mutable.Map(("a", 4), ("b", 5), ("c", 6))

    // 封装广播变量
    val bc: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)

    rdd1.map {
      case (w, c) => {
        // 方法广播变量
        val l: Int = bc.value.getOrElse(w, 0)
        (w, (c, l))
      }
    }.collect().foreach(println)


    sc.stop()

  }
}

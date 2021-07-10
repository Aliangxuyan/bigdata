package com.lxy.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author lxy
 * @date 2021/7/7
 *       Broadcast 广播
 */
object Spark05_Bc {
  def main(args: Array[String]): Unit = {

    val sparConf = new SparkConf().setMaster("local").setAppName("Acc")
    val sc = new SparkContext(sparConf)

    val rdd1 = sc.makeRDD(List(
      ("a", 1), ("b", 2), ("c", 3)
    ))
    //        val rdd2 = sc.makeRDD(List(
    //            ("a", 4),("b", 5),("c", 6)
    //        ))
    val map = mutable.Map(("a", 4), ("b", 5), ("c", 6))



    // join会导致数据量几何增长，并且会影响shuffle的性能，不推荐使用
    //val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
    //joinRDD.collect().foreach(println)
    // (a, 1),    (b, 2),    (c, 3)
    // (a, (1,4)),(b, (2,5)),(c, (3,6))

    /**
     * 特殊的方式实现了 join 操作，但是数据量大的时候需要谨慎使用
     *
     * 如果有 10个task 的话，都应该执行map算子 中的操作，所以每个task 中都应该有这个map 数据，
     * 闭包数据都是以task 为单位发送的，每个任务中都包含闭包数据，这样可能会导致，一个Excutor 中
     * 包含大量的重复的数据，并且占用大量的内存空间，
     *
     * Excutor 其实就是一个JVM ，所以在启动的时候，会自动分配内存，完全可以将任务重的闭包数据放到EXcutor 的内存中
     * 达到共享的目的； spark 中的广播变量可以实现将闭包数据放到 Excutor的内存中了，不能够进行更改（分布式共享的只读变量）
     */
    rdd1.map {
      case (w, c) => {
        val l: Int = map.getOrElse(w, 0)
        (w, (c, l))
      }
    }.collect().foreach(println)

    sc.stop()

  }
}

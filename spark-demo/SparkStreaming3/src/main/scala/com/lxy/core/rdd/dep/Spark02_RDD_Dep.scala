package com.lxy.core.rdd.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author lxy
 * @date 2021/7/7
 *
 *       血缘关系 toDebugString
 *       相邻RDD 的依赖关系：
 *       lines.dependencies：打印 宽依赖 ShuffleDependency & 窄依赖 OneToOneDependency
 *
 *       OneToOneDependency[T](rdd: RDD[T]) extends NarrowDependency
 *
 */
object Spark02_RDD_Dep {
  def main(args: Array[String]): Unit = {

    val sparConf = new SparkConf().setMaster("local").setAppName("Dep")
    val sc = new SparkContext(sparConf)

    val lines: RDD[String] = sc.textFile("datas/word.txt")
    println(lines.dependencies)
    println("*************************")
    val words: RDD[String] = lines.flatMap(_.split(" "))
    println(words.dependencies)
    println("*************************")
    val wordToOne = words.map(word => (word, 1))
    println(wordToOne.dependencies)
    println("*************************")
    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
    println(wordToSum.dependencies)
    println("*************************")
    val array: Array[(String, Int)] = wordToSum.collect()
    array.foreach(println)

    sc.stop()

  }
}

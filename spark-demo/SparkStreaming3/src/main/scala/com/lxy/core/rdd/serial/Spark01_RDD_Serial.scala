package com.lxy.core.rdd.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author lxy
 * @date 2021/7/7
 *
 *       5.1.4.6RDD 序列化
 *       从计算的角度, 算子以外的代码都是在Driver 端执行, 算子里面的代码都是在 Executor 端执行。
 *       那么在 scala 的函数式编程中，就会导致算子内经常会用到算子外的数据，这样就形成了闭包的效果，如果使用的算子外的数据无法序列化，就意味着无法传值给Executor 端执行，就会发生错误，
 *       所以需要在执行任务计算前，检测闭包内的对象是否可以进行序列化，这个操作我们称之为闭包检测。
 *       Scala2.12 版本后闭包编译方式发生了改变
 *
 */
object Spark01_RDD_Serial {
  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)

    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))

    val search = new Search("h")

    //    search.getMatch1(rdd).collect().foreach(println)
    search.getMatch2(rdd).collect().foreach(println)

    sc.stop()
  }

  // 查询对象
  // 类的构造参数其实是类的属性, 构造参数需要进行闭包检测，其实就等同于类进行闭包检测，
  // 可以将target 中的class 反编译看到 query 是 Search 类的属性
  //  class Search(query: String) extends Serializable {
  //  case class Search(query: String) {

  class Search(query: String) {

    def isMatch(s: String): Boolean = {
      s.contains(this.query)
    }

    // 函数序列化案例，外置函数，注意：闭包检测，外置函数中的对象没有序列化, class Search(query: String) extends Serializable  解决问题，
    def getMatch1(rdd: RDD[String]): RDD[String] = {
      rdd.filter(isMatch)
    }

    // 属性序列化案例，匿名函数
    def getMatch2(rdd: RDD[String]): RDD[String] = {
      // 可以将query  赋值给s ，s 此时和 Search 没有关系，只是一个字符串，所以不需要序列化
      val s = query
      rdd.filter(x => x.contains(s))
    }
  }

}

package com.lxy.practice

import java.util

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author lxy
  * @date 2019-12-07
  */
class CustomAccu extends AccumulatorV2[String, java.util.Set[String]] {

  private val _logArray: java.util.Set[String] = new util.HashSet[String]()

  // 分区中的暂存变量是否为空
  override def isZero: Boolean = {
    _logArray.isEmpty
  }

  //复制一个对象
  override def copy(): AccumulatorV2[String, util.Set[String]] = {
    val newAcc = new CustomAccu
    _logArray.synchronized {
      newAcc._logArray.addAll(_logArray)
    }
    newAcc
  }

  //重启你的对象状态
  override def reset(): Unit = {
    _logArray.clear()
  }

  //在分区中增加数据
  override def add(v: String): Unit = {
    _logArray.add(v)
  }

  //将多个分区的累加器累加
  override def merge(other: AccumulatorV2[String, util.Set[String]]): Unit = {
    other match {
      case o: CustomAccu => _logArray.addAll(o.value)
    }
  }

  //读取最终的值
  override def value: util.Set[String] = ???
}

object logAccu {
  def main(args: Array[String]): Unit = {
    // 创建sparkCOnf 对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("Practice")

    // 创建spackContext 对象
    val sc = new SparkContext(conf)

    //创建自定义累加器并注册示例
    val accu = new CustomAccu
    sc.register(accu, "accu")

    val rdd = sc.makeRDD(Array("1", "2", "3a", "4a", "5a", "6", "7"))

    rdd.filter { line =>
      if (line.endsWith("a")) {
        accu.add(line)
      }
      true
    }.collect()

   accu.value

    sc.stop()
  }

}
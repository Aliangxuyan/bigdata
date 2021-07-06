package com.lxy.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession, TypedColumn, functions}
import org.apache.spark.sql.expressions.Aggregator


/**
 * @author lxy
 * @date 2021/6/28
 * 3。0 版本之后，这种强类型的用法
 *
 * 早期版本中，sqark 不能使用强类型的方法,使用 dsl语法操作
 *
 * 高版本中用 UserDefinedAggregateFunction 已废弃
 * udaf
 *
 * -IN, 输入的数据类型
 * BUF, 缓冲区数据类型
 * OUT 输出的数据类型
 *
 * 固定写法：
 * Encoders.product ：自定义的类型
 * Encoders.scalaLong：scala 自带的类型
 *
 *
 * spark 3.0 之前 强类型 dsl 方式实现 Aggregator ，但是实现方式不一致
 *
 */
//case class Buff(var total:Long,var count:Long)
case class User(var name:String,var salary:Long)

class MyAverage3 extends Aggregator[User,Buff,Long]{

  // 初始值或者0值,缓冲区的初始值
  override def zero: Buff = {
    Buff(0L,0L)
  }

  // 根据并输入的数据更新缓冲区的数据
  override def reduce(buff: Buff, in: User): Buff = {
    buff.total = buff.total + in.salary
    buff.count = buff.count + 1
    buff
  }

  // 合并缓冲区
  override def merge(buff1: Buff, buff2: Buff): Buff = {
    buff1.total = buff1.total + buff2.total
    buff1.count = buff1.count + buff2.count
    buff1
  }

  // 计算结果
  override def finish(buff: Buff): Long = {
    buff.total / buff.count
  }

  // 缓冲区的编码操作
//  override def bufferEncoder: Encoder[Buff] = Encoders.product

  // 输出的编码格式
//  override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  override def bufferEncoder: Encoder[Buff] = Encoders.product

  override def outputEncoder: Encoder[Long] = Encoders.scalaLong
}

object Test03{

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("udaf1").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    // 倒入 spark 变量的隐式转换
    import spark.implicits._

    val df = spark.read.json("/Users/lxy/Desktop/bigdata/09_Spark/资料/Spark教程/2.code/spark/sparkSql/doc/employees.json")
    val ds:Dataset[User] = df.as[User]

    var udafCol:TypedColumn[User,Long] = new MyAverage3().toColumn

    ds.select(udafCol).show()
//    df.createOrReplaceTempView("employee")
//    spark.sql("select average(salary) as avg from employee").show()

    spark.stop()

  }

}

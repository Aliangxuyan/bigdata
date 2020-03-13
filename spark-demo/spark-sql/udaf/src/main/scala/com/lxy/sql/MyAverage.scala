package com.lxy.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * 自定义聚合函数
  */
class MyAverage extends UserDefinedAggregateFunction{

  //聚合函数输入的数据类型
  override def inputSchema: StructType = StructType(StructField("salary",LongType)::Nil)

  //小范围聚合临时变量的类型
  override def bufferSchema: StructType = StructType(StructField("sum",LongType)::StructField("count",LongType)::Nil)

  //返回值得类型
  override def dataType: DataType = DoubleType

  //幂性等
  override def deterministic: Boolean = true

  //初始化你的数据结构
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  //每一个分区去更新数据结构
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  //将所有分区的数据合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  //计算值
  override def evaluate(buffer: Row): Double = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }

}

object Test{

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("udaf").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    // 倒入 spark 变量的隐式转换
    import spark.implicits._

    //注册
    spark.udf.register("average", new MyAverage())

    val df = spark.read.json("/Users/lxy/Desktop/bigdata/09_Spark/资料/Spark教程/2.code/spark/sparkSql/doc/employees.json")
    df.createOrReplaceTempView("employee")
    spark.sql("select average(salary) as avg from employee").show()

    spark.stop()

  }

}

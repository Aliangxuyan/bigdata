package com.lxy.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author lxy
  * @date 2019-12-08
  */
object HelloWorld {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("spark-sql")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val sc = spark.sparkContext

    val df = spark.read.json("/Users/lxy/Desktop/bigdata/09_Spark/资料/Spark教程/2.code/spark/sparkSql/doc/employees.json")

    // 注意，倒入隐式转换
    import spark.implicits._

    //展示整张表
    df.show()

    //展示整张表的scheam
    df.printSchema()

    // DSL 风格查询
    df.filter($"salary" > 3000).show()

    // 注册一个表名
    df.createOrReplaceTempView("employees")

    // 查询
    val result = spark.sql("select * from employees where salary > 3000")


    result.write.json("./ab.json")

    spark.close()


  }

}

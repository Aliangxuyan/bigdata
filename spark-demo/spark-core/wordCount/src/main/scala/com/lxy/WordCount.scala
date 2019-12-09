package com.lxy

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author lxy
  * @date 2019-12-05
  */
object WordCount {
  def main(args: Array[String]): Unit = {

    //新建sparkconf对象
    //    IDEA不能直接通过代码提交到 spark的 standalone模式。 一般只是通过local[*]模式来做调试
    //        val conf = new SparkConf().setMaster("spark://master01:7077").setAppName("wordcount")
    //    val conf = new SparkConf().setMaster("local[*]").setAppName("wordcount")

    // master 一般是以参数传入
    val conf = new SparkConf().setAppName("wordcount")
    //创建spark'Context
    val sc = new SparkContext(conf)

    //读取数据(从hdfs也可以从本地读取)
    val textfile = sc.textFile("hdfs://hadoop102:9000/spark/RELEASE")

    //按空格切分
    val words = textfile.flatMap(_.split(" "))

    //转换为kv结构
    val k2v = words.map((_, 1))

    //将相同key的合并
    val result = k2v.reduceByKey(_ + _)


    //输出结果
    result.collect().foreach(println _)

    result.saveAsObjectFile("hdfs://hadoop102:9000/spark/RELEASE-result")

    //关闭连接
    sc.stop()
  }
}

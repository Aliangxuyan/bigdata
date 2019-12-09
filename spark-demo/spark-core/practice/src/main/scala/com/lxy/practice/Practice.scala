package com.lxy.practice

import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

/**
  * @author lxy
  * @date 2019-12-07
  */
object Practice {

  def getHour(timelong: String): String = {
    val datetime = new DateTime(timelong.toLong)
    datetime.getHourOfDay.toString
  }

  def main(args: Array[String]): Unit = {

    //需求：统计每一个省份点击TOP3的广告ID    省份+广告 **************************************8

    // 创建sparkCOnf 对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("Practice")

    // 创建spackContext 对象
    val sc = new SparkContext(conf)

    // 读取数据RDD【String】
    val logs = sc.textFile("/opt/bigdata/spark-2.1.1-bin-hadoop2.7/tmpdata/agent.log")

    // 将RDD 的string 转换为数组RDD[Array[string]]
    val logsArray = logs.map(x => x.split(" "))

    //提取相应的数据，转换粒度 RDD【（pro_adid,1)】
    val proAndAd2count = logsArray.map(x => (x(1) + "_" + x(4), 1))

    // 将每一个省份每一个广告的所有点击量聚合 RDD[pro_adid,sum]
    val proAndAd2sum = proAndAd2count.reduceByKey((x, y) => x + y)

    // 将粒度扩大，拆分开 RDD[pro,(sum,adid)]
    val pro2AdSum = proAndAd2sum.map { x => val param = x._1.split("_"); (param(0), (x._2, param(1))) }

    // 将每一个省份所有的广告合并成为一个数组 RDD(pro,Array[(sum,addid),(sum1,adid2).......])
    val pro2AdArray = pro2AdSum.groupByKey()

    // 排序取前3
    val result = pro2AdArray.mapValues(values => values.toList.sortWith((x, y) => (x._1 > y._1)).take(3))
    // 行动操作
    println(result.collectAsMap());

    // 需求：统计每一个省份每一个小时的TOP3广告的ID ************************************8

    // 产生最小粒度 RDD[(pro_hour_ad,1)]
    val pro_hour_ad2Count = logsArray.map {
      x => (x(1) + "_" + getHour(x(0)) + "_" + x(4), 1)
    }

    // 计算每个省份每个小时每个广告的点击量 RDD[(pro_hour_ad,sum)]
    val pro_hour_ad2Sum = pro_hour_ad2Count.reduceByKey(_ + _)

    // 拆分key ，扩大粒度  RDD[pro_hour,(ad,sum)]
    val pro_hour_ad2Array = pro_hour_ad2Sum.map { x =>
      val param = x._1.split("_")
      (param(0) + "_" + param(1), (param(2), x._2))
    }
    // 将一个省份一个小时内的数据聚集RDD[(pro_hour,array(add,sum))]
    val pro_hour_ad2Group = pro_hour_ad2Array.groupByKey()

    // 直接对一个小时内的广告排序，取前3
    val pro_hour2Top3AdArray = pro_hour_ad2Group.mapValues {
      x => x.toList.sortWith((x, y) => (x._2 > y._2)).take(3)
    }

    //扩大粒度 RDD[(pro,(hour,Array[(add,sum))]]
    val pro2HourAdArray = pro_hour2Top3AdArray.map {
      x =>
        val param = x._1.split("_")
        (param(0), (param(1), x._2))
    }
    val result2 = pro2HourAdArray.groupByKey()

    result2.collectAsMap()

    // 关闭创建spackContext
    sc.stop()
  }

}

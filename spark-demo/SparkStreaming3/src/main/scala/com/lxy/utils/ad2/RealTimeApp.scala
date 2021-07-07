package com.lxy.utils.ad2

import java.sql.Connection

import com.lxy.utils.PropertiesUtil
import com.lxy.utils.ad.{Ads_log, BlackListHandler, JdbcUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author lxy
 * @date 2021/7/7
 *
 *       CREATE TABLE area_city_ad_count ( dt VARCHAR(255),
 *       area VARCHAR(255), city VARCHAR(255), adid VARCHAR(255),
 *       count BIGINT,
 *       PRIMARY KEY (dt,area,city,adid)
 *       );
 */
class RealTimeApp {
  def main(args: Array[String]): Unit = {

    //1.创建 SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RealTimeApp")

    //2.创建 StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //3.读取 Kafka 数据 1583288137305 华南 深圳 4 3
    val topic: String = PropertiesUtil.load("config.properties").getProperty("kafka.topic")
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc)

    //4.将每一行数据转换为样例类对象
    val adsLogDStream: DStream[Ads_log] = kafkaDStream.map(record => {
      //a.取出 value 并按照" "切分
      val arr: Array[String] = record.value().split(" ")
      //b.封装为样例类对象
      Ads_log(arr(0).toLong, arr(1), arr(2), arr(3), arr(4))
    })

    //5.根据 MySQL 中的黑名单表进行数据过滤
    val filterAdsLogDStream: DStream[Ads_log] = adsLogDStream.filter(adsLog => {
      //查询 MySQL,查看当前用户是否存在。
      val connection: Connection = JdbcUtil.getConnection
      val bool: Boolean = JdbcUtil.isExist(connection, "select * from black_list where userid=?", Array(adsLog.userid))
      connection.close()
      !bool
    })

    filterAdsLogDStream.cache()

    //6.对没有被加入黑名单的用户统计当前批次单日各个用户对各个广告点击的总次数,
    // 并更新至 MySQL
    // 之后查询更新之后的数据,判断是否超过 100 次。
    // 如果超过则将给用户加入黑名单
    BlackListHandler.saveBlackListToMysql(filterAdsLogDStream)

    //7.统计每天各大区各个城市广告点击总数并保存至 MySQL 中
    DateAreaCityAdCountHandler.saveDateAreaCityAdCountToMysql(filterAdsLogDStream)

    //10.开启任务
    ssc.start()
    ssc.awaitTermination()
  }
}

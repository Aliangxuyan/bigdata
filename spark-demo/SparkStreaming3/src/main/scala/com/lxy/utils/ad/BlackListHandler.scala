package com.lxy.utils.ad

import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.streaming.dstream.DStream

/**
 * @author lxy
 * @date 2021/7/7
 *
 *       CREATE TABLE black_list (userid CHAR(1) PRIMARY KEY);
 *
 *       CREATE TABLE user_ad_count ( dt varchar(255),
 *       userid CHAR (1),
 *       adid CHAR (1), count BIGINT,
 *       PRIMARY KEY (dt, userid, adid)
 *       );
 */
object BlackListHandler {

  def saveBlackListToMysql(filterAdsLogDStream: DStream[Ads_log]) = ???

  //时间格式化对象
  private val sdf = new SimpleDateFormat("yyyy-MM-dd")

  def addBlackList(filterAdsLogDSteam: DStream[Ads_log]): Unit = {

    //统计当前批次中单日每个用户点击每个广告的总次数
    //1.将数据接转换结构 ads_log=>((date,user,adid),1)
    val dateUserAdToOne: DStream[((String, String, String), Long)] = filterAdsLogDSteam.map(adsLog => {
      //a.将时间戳转换为日期字符串
      val date: String = sdf.format(new Date(adsLog.timestamp))
      //b.返回值
      ((date, adsLog.userid, adsLog.adid), 1L)
    })

    //2.统计单日每个用户点击每个广告的总次数((date,user,adid),1)=>((date,user,adid),count)
    val dateUserAdToCount: DStream[((String, String, String), Long)] = dateUserAdToOne.reduceByKey(_ + _)
    dateUserAdToCount.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {

        val connection: Connection = JdbcUtil.getConnection

        iter.foreach { case ((dt, user, ad), count) => JdbcUtil.executeUpdate(connection,
          """
            |INSERT INTO user_ad_count (dt,userid,adid,count)
            |VALUES (?,?,?,?)
            |ON DUPLICATE KEY
            |UPDATE count=count+?
""".stripMargin, Array(dt, user, ad, count, count))
          val ct: Long = JdbcUtil.getDataFromMysql(connection, "select count from user_ad_count where dt=? and userid=? and adid =?", Array(dt, user, ad))
          if (ct >= 30) {
            JdbcUtil.executeUpdate(connection, "INSERT INTO black_list (userid) VALUES (?) ON DUPLICATE KEY update userid=?", Array(user, user))
          }
        }
        connection.close()
      })
    })
  }

  def filterByBlackList(adsLogDStream: DStream[Ads_log]): DStream[Ads_log] = {
    adsLogDStream.transform(rdd => {
      rdd.filter(adsLog => {
        val connection: Connection = JdbcUtil.getConnection
        val bool: Boolean = JdbcUtil.isExist(connection, "select * from black_list where userid=?", Array(adsLog.userid))
        connection.close()
        !bool
      })
    })
  }

}

package com.lxy.loginfaildetect

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @author lxy
  *         2020/2/13
  *
  *         CEP （复杂时间处理）
  */
object LoginFailWithCep {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //1、 读取事件数据,创建简单时间流
    val resource = getClass.getResource("/LoginLog.csv")
    val loginEventStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        LoginEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
      })

    val warningStream = loginEventStream
      .keyBy(_.userId) // 以用户ID 做为分组

    // 2、定义匹配模式,连续两次登录失败，严格紧邻  followedBy() 宽松紧邻，该需求需要严格紧邻
    val loginFailPattern = Pattern.begin[LoginEvent]("begin").where(_.eventType == "fail")
      .next("next").where(_.eventType == "fail")
      .within(Time.seconds(2))

    // 3、在时间流上应用模式，得到一个pattern stream
    val patternStream = CEP.pattern(loginEventStream, loginFailPattern)

    // 从pattern stream  应用select function，检出匹配的时间序列
    val loginFailDataStream = patternStream.select(new LoginFailMatch())

    loginFailDataStream.print()
    env.execute("LoginFail with cep job ")
  }

  class LoginFailMatch() extends PatternSelectFunction[LoginEvent, Warning] {
    override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
      // 从map  中按照名称取出对应的事件
      val firstFail = map.get("begin").iterator.next()
      val lastFail = map.get("next").iterator().next()
      Warning(firstFail.userId, firstFail.eventTime, lastFail.eventTime, "login fail")

    }
  }

}

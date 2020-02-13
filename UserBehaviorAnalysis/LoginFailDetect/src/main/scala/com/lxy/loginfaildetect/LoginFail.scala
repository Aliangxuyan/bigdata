package com.lxy.loginfaildetect


import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * @author lxy
  *         2020/2/12
  */
// 输入的登录时间样例类
case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

// 输出的异常报警信息样例类
case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)


object LoginFail {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取事件数据
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
      .process(new LoginWarning(2))

    warningStream.print()
    env.execute("LoginFail job")

  }

  class LoginWarning(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning] {
    // 保存状态，保存2秒内的所有的登录失败事件
    lazy val loginFailState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-fail-state", classOf[LoginEvent]))

    override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
      //      // 判断类型是否是fail，只添加fail 的事件到状态
      //      if (value.eventType == "fail") {
      //        val loginFailList = loginFailState.get()
      //        // 如果是第一个失败的状态，注册定时器
      //        if (!loginFailList.iterator().hasNext) {
      //          ctx.timerService().registerEventTimeTimer(value.eventTime * 1000 + 2000) // 当前时间+ 2s
      //        } else {
      //          // 如果是成功，直接清空状态
      //          loginFailState.clear()
      //        }
      //        loginFailState.add(value)
      //      }

      if (value.eventType == "fail") {
        // 如果是失败，判断之前是否有登录失败的事件
        val iter = loginFailState.get().iterator()
        if (iter.hasNext) {
          // 如果已经有登录失败事件，那么就判断事件是否在2S 之内
          val firstFail = iter.next()
          if (value.eventTime < firstFail.eventTime + 2) {
            // 如果两次间隔小于 2S  输出报警
            out.collect(Warning(value.userId, firstFail.eventTime, value.eventTime, "login fail in 2 seconds for "))
          }
          // 单个的时候可以先清空再增加进去，更新最近一次的登录失败事件，保存在状态里面
          loginFailState.clear()
          loginFailState.add(value)
        } else {
          // 如果是第一次登录失败，直接添加到状态
          loginFailState.add(value)
        }
      } else {
        // 如果是成功清空状态
        loginFailState.clear()
      }
    }

    // 定时器
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
      // 触发定时器的时候，根据状态里的失败个数决定是否输出报警
      val allLoginFails: ListBuffer[LoginEvent] = new ListBuffer[LoginEvent]()
      val iter = loginFailState.get().iterator()
      while (iter.hasNext) {
        allLoginFails += iter.next()
      }
      // 判断个数
      if (allLoginFails.length >= maxFailTimes) {
        out.collect(Warning(allLoginFails.head.userId, allLoginFails.head.eventTime, allLoginFails.last.eventTime, "login fail in 2 seconds for " + allLoginFails.length + " times."))
      }
      // 清空状态
      loginFailState.clear()
    }
  }

}

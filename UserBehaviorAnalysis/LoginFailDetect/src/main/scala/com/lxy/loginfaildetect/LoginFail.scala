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
  *
  *         6、恶意登录监控
  *         • 基本需求
  *         – 用户在短时间内频繁登录失败，有程序恶意攻击的可能
  *         – 同一用户(可以是不同IP)在2秒内连续两次登录失败，需要报警
  *
  *         • 解决思路
  *         – 将用户的登录失败行为存入 ListState，设定定时器2秒后触发，查看 ListState 中有几次失败登录
  *         – 更加精确的检测，可以使用 CEP 库实现事件流的模式匹配
  *
  *
  *         可以设置定时器，但是定时器触发的这段时间可能会漏掉一些成功失败的访问，所有不用定时器可以直接进行判断两次连续失败是否在2S 之内
  *
  *         下面是连续 2次的问题，如果是3 或者4  的话可以判断状态编程里面已经有几个，都要进行判断，
  *         对于有序和稍微乱序数据比较好实现，但是对于乱序数据连续多次登录失败的问题，下面实现不OK ，需要用CEP 来实现复杂事件
  *
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
      // 分配时间戳，log日志中存在乱序，可以设置watermark 设置延迟，延迟时间根据自己的需求设置
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
      //
      // 不能等到2S 之后做报警输出，又可能最后一次输出是success ，应该错误2次之后直接判断是否在2S 之内直接输出，这样可以不需要定时器
      // 判断类型是否是fail，只添加fail 的事件到状态
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

      // 不能直接在外面 fiter 中进行过滤，因为两次fail 2S 之内可能存在success 的情况，这样会存在问题
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

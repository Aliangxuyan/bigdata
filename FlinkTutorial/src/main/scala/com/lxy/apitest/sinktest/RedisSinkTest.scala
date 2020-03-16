package com.lxy.apitest.sinktest

import com.lxy.apitest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}


/**
  * @author lxy
  * @date 2020-02-04
  */
object RedisSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // source
    val inputStream = env.readTextFile("/Users/lxy/Documents/Idea_workspace/bigdata/FlinkTutorial/src/main/resources/sensor.txt")

    // transform
    val dataStream = inputStream
      .map(
        data => {
          val dataArray = data.split(",")
          SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
        }
      )

    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost("localhost")
      .setPort(6379)
      .build()

    // sink
    dataStream.addSink(new RedisSink(conf, new MyRedisMapper()))

    env.execute("redis sink test")
  }
}

class MyRedisMapper() extends RedisMapper[SensorReading] {

  // 定义保存数据到redis的命令
  override def getCommandDescription: RedisCommandDescription = {
    // 把传感器id和温度值保存成哈希表 HSET key field value
    new RedisCommandDescription(RedisCommand.HSET, "sensor_temperature")
  }

  // 定义保存到redis的value
  override def getValueFromData(t: SensorReading): String = t.temperature.toString

  // 定义保存到redis的key
  override def getKeyFromData(t: SensorReading): String = t.id
}

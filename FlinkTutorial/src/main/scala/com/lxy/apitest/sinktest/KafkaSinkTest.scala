package com.lxy.apitest.sinktest

import java.util.Properties

import com.lxy.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011.Semantic
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

/**
  * @author lxy
  * @date 2020-02-04
  *
  *      def print(): DataStreamSink[T] = stream.print()
  *      dataStream.print()  print() 也是一种sink
  *
  *
  *      一般真实的情况多是处理实时数据
  *      source : kafka
  *      sink :kafka | es | redis | jdbc eg...
  */
object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // source
    //        val inputStream = env.readTextFile("/Users/lxy/Documents/Idea_workspace/bigdata/FlinkTutorial/src/main/resources/sensor.txt")
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val inputStream = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))

    // Transform操作

    val dataStream = inputStream
      .map(
        data => {
          val dataArray = data.split(",")
          SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble).toString // 转成String方便序列化输出
        }
      )

    // sink
    dataStream.addSink(new FlinkKafkaProducer011[String]("sinkTest", new SimpleStringSchema(), properties))
    dataStream.print()

    env.execute("kafka sink test")
  }
}

package com.lxy.hotitems_analysis

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * @author lxy
  *  2020-02-11
  */
object KafkaProducer {

  def writeToKafka(topic: String): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("auto.offset.reset", "latest")

    // 定义一个kafka producer
    val producer = new KafkaProducer[String, String](properties)

    // 从文件中读取数据，发送
    val bufferedSource = io.Source.fromFile("/Users/lxy/Documents/Idea_workspace/bigdata/UserBehaviorAnalysis/HotItemsAnalysis/src/main/resources/UserBehavior.csv")
    for (line <- bufferedSource.getLines()) {
      val record = new ProducerRecord[String, String](topic, line)
      producer.send(record)
    }
    producer.close()
  }

  def main(args: Array[String]): Unit = {
    writeToKafka("hotitems")
  }
}

package com.lxy.hotitems_analysis

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import net.sf.json.JSONObject

/**
  * @author lxy
  *         2020-02-11
  */
object kfk_pro {

  def writeToKafka(topic: String): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "kafka-dianxiao-130-104.bjdd.zybang.com:9092,kafka-dianxiao-130-111.bjdd.zybang.com:9092,kafka-dianxiao-130-115.bjdd.zybang.com:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("auto.offset.reset", "latest")

    // 定义一个kafka producer
    val producer = new KafkaProducer[String, String](properties)

    // 从文件中读取数据，发送
    val bufferedSource = io.Source.fromFile("/tmp/laxin_activity_chanals_job-20200613")

    for (line <- bufferedSource.getLines()) {
      val dataArray = line.split("\t");
      val json = new JSONObject()
      json.put("channel_id", dataArray(0));
      json.put("activity_id", dataArray(1));
      json.put("unionid", dataArray(2));
      json.put("event_id", dataArray(3));
      json.put("event_time", dataArray(4).trim.toLong);
      json.put("create_time", dataArray(5));
      json.put("act_type", dataArray(6));
      json.put("wxid", "");
      json.put("wx_group_id", "");
      println("****" + json.toString)
      val record = new ProducerRecord[String, String](topic, json.toString())
      producer.send(record)
    }
    producer.close()
  }

  def main(args: Array[String]): Unit = {
    writeToKafka("yk_laxindata_wx_activity")
  }
}

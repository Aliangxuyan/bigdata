package com.lxy.clickhouse

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.Array.range
import scala.util.Random.shuffle

/**
 * @author lxy
 * @date 2020/12/1
 */
object KafkaProducers {
  def main(args: Array[String]): Unit = {
    SendtoKafka("test")
  }

  def SendtoKafka(topic: String): Unit = {
    val pro = new Properties()
    pro.put("bootstrap.servers", "127.0.0.1:9092")
    pro.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    pro.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](pro)
    var member_id = range(1, 10)
    var goods = Array("Milk", "Bread", "Rice", "Nodles", "Cookies", "Fish", "Meat", "Fruit", "Drink", "Books", "Clothes", "Toys")
    //var ts=DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss",Locale.CHINA).format( ZonedDateTime.now())
    while (true) {
      var ts = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())
      var msg = shuffle(member_id.toList).head + "\t" + shuffle(goods.toList).head + "\t" + ts + "\t" + "\n"
      print(msg)
      var record = new ProducerRecord[String, String](topic, msg)
      producer.send(record)
      Thread.sleep(2000)
    }
    //val source=Source.fromFile("C:\\UserBehavior.csv")
    //for (line<-source.getLines()){
    // val record=new ProducerRecord[String,String](topic,line)

    //print(ts)
    producer.close()

  }

}

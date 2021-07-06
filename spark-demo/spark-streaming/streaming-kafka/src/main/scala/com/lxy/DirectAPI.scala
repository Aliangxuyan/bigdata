package com.lxy
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author lxy
 * @date 2021/7/6
 * 通过 SparkStreaming 从Kafka 读取数据，并将读取过来的数据做简单计算
 */
object DirectAPI {

  def main(args: Array[String]): Unit = {

    //1.创建 SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("ReceiverWordCount").setMaster("local[*]")

    //2.创建 StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //3.定义 Kafka 参数
    val kafkaPara: Map[String, Object] = Map[String, Object]( ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG ->
      "linux1:9092,linux2:9092,linux3:9092", ConsumerConfig.GROUP_ID_CONFIG -> "lxy", "key.deserializer" ->
      "org.apache.kafka.common.serialization.StringDeserializer", "value.deserializer" ->
      "org.apache.kafka.common.serialization.StringDeserializer"
    )

    //4.读取 Kafka 数据创建 DStream
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Set("lxy"), kafkaPara))

    //5.将每条消息的 KV 取出
    val valueDStream: DStream[String] = kafkaDStream.map(record => record.value())
      //6. 计 算 WordCount
      valueDStream.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print()

    //7. 开 启 任 务
    ssc.start()
    ssc.awaitTermination()
  }
}

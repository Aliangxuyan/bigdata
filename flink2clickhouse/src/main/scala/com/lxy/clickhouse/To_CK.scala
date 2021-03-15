package com.lxy.clickhouse

/**
 * @author lxy
 * @date 2020/12/1
 */

import java.sql.PreparedStatement
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.descriptors.Kafka


//当前版本的 flink-connector-jdbc，使用 Scala API 调用 JdbcSink 时会出现 lambda 函数的序列化问题。我们只能采用手动实现 interface 的方式来传入相关 JDBC Statement build 函数
class CkSinkBuilder extends JdbcStatementBuilder[(Int, String, String)] {
  def accept(ps: PreparedStatement, v: (Int, String, String)): Unit = {
    ps.setInt(1, v._1)
    ps.setString(2, v._2)
    ps.setString(3, v._3)
  }
}

object To_CK {
  def main(args: Array[String]): Unit = {

    //获得环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1) //设置并发为1，防止打印控制台乱序
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //Flink 默认使用 ProcessingTime 处理,设置成event time
    //    val tEnv = StreamTableEnvironment.create(env) //Table Env 环境
    //从Kafka读取数据
    val pros = new Properties()
    pros.setProperty("bootstrap.servers", "127.0.0.1:9092")
    pros.setProperty("group.id", "test")
    pros.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    pros.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    pros.setProperty("auto.offset.reset", "latest")
    import org.apache.flink.api.scala._
    val dataSource = env.addSource(new FlinkKafkaConsumer[String]("test", new SimpleStringSchema(), pros))
    val sql = "insert into ChinaDW.testken(userid,items,create_date)values(?,?,?)"
    val result = dataSource.map(line => {
      val x = line.split("\t")
      //print("收到数据",x(0),x(1),x(2),"\n")
      val member_id = x(0).trim.toLong
      val item = x(1).trim
      val times = x(2).trim
      var time = 0l
      try {
        time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(times).getTime
      } //时间戳类型
      catch {
        case e: Exception => {
          print(e.getMessage)
        }
      }
      (member_id.toInt, item.toString, time.toLong)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(Int, String, Long)](Time.seconds(2)) {
      override def extractTimestamp(t: (Int, String, Long)): Long = t._3
    }).map(x => {
      (x._1, x._2, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(x._3))
    }) //时间还原成datetime类型
    //result.print()
    result.print()
    result.addSink(JdbcSink.sink[(Int, String, String)]
      (sql,
        new CkSinkBuilder,
        new JdbcExecutionOptions.Builder().withBatchSize(5).build(),
        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
          .withUrl("jdbc:clickhouse://127.0.0.1:8123")
          .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
          .withUsername("default")
          .build()
      ))


    env.execute("To_CK")
  }

}
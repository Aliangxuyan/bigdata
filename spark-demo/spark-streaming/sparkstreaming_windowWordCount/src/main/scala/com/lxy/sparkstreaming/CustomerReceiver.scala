package com.lxy.sparkstreaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

//需要继承一个类
class CustomerReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

  //接收器启动的时候
  override def onStart(): Unit = {

    new Thread("socket Receiver") {
      override def run(): Unit = {
        reveice()
      }

    }

  }

  def reveice(): Unit = {

    //创建Socket连接
    var socket: Socket = null
    var input: String = null

    try {

      socket = new Socket(host, port)

      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))

      //读取第一条数据
      input = reader.readLine()

      while (!isStopped() && input != null) {
        //数据提交给框架
        store(input)

        input = reader.readLine()
      }

      reader.close()
      socket.close()

      restart("restart")

    } catch {
      case e: java.net.ConnectException => restart("restart")
      case t: Throwable => restart("restart")
    }

  }


  //接收器停止的时候，主要做资源的销毁
  override def onStop(): Unit = {}
}


object CountWorld1 {
  def main(args: Array[String]): Unit = {

    //创建SparkConf对象
    val conf = new SparkConf().setAppName("Streaming").setMaster("local[*]")

    //创建StreamingContext对象
    val ssc = new StreamingContext(conf, Seconds(5))

    // 需要有状态必须设置checkpoint,一般放在HDFS  上面
    ssc.checkpoint("./checkpoint")

    //创建一个接收器来接受数据 DStream[String]
    //val linesDSteam = ssc.socketTextStream("master01",9999)

    val linesDStream = ssc.receiverStream(new CustomerReceiver("master01", 9999))

    // transform  Dstream 和 rdd  之间的转换，里面可以直接像下面一样直接RDD 操作，，和下面的注释内容功能实现一样

    //    val result = linesDStream.transform { rdd =>
    //      val words = rdd.flatMap(_.split(" "))
    //      val kv = words.map((_, 1))
    //      val result = kv.reduceByKey(_ + _)
    //      result
    //    }


    //flatMap转换成为单词 DStream[String]
    val wordsDStream = linesDStream.flatMap(_.split(" "))

    //将单词转换为KV结构 DStream[(String,1)]
    val kvDStream = wordsDStream.map((_, 1))

    //将相同单词个数进行合并
    //     val kvDStream2 = kvDStream.reduceByKey(_ + _)

    //************************************** updateStateByKey
    val updateFunc = (value: Seq[Int], state: Option[Int]) => {
      value.sum
      val previousState = state.getOrElse(0)
      Some(previousState + value.sum)
    }
    val result = kvDStream.updateStateByKey[Int](updateFunc)

    result.print()

    ssc.start()
    ssc.awaitTermination()

  }
}

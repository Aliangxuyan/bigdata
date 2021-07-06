package com.lxy

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.streaming.{StreamingContext, StreamingContextState}

/**
 * @author lxy
 * @date 2021/7/6
 *
 * 优雅的关闭
 */
class MonitorStop(ssc: StreamingContext) extends Runnable {
  override def run(): Unit = {

    val fs: FileSystem = FileSystem.get(new URI("hdfs://linux1:9000"), new Configuration(), "atguigu")

    while (true) {
      try
        Thread.sleep(5000)
      catch {
        case e: InterruptedException => e.printStackTrace()
      }
      val state: StreamingContextState = ssc.getState
      val bool: Boolean = fs.exists(new Path("hdfs://linux1:9000/stopSpark"))
      if (bool) {
        if (state == StreamingContextState.ACTIVE) {
          ssc.stop(stopSparkContext = true, stopGracefully = true)
          System.exit(0)
        }
      }
    }
  }
}

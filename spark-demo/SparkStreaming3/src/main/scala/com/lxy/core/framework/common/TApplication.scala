package com.lxy.core.framework.common
import com.lxy.core.framework.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}
/**
 * @author lxy
 * @date 2021/7/7
 */
trait TApplication {
  def start(master: String = "local[*]", app: String = "Application")(op: => Unit): Unit = {
    val sparConf = new SparkConf().setMaster(master).setAppName(app)
    val sc = new SparkContext(sparConf)
    EnvUtil.put(sc)

    try {
      op
    } catch {
      case ex => println(ex.getMessage)
    }

    // TODO 关闭连接
    sc.stop()
    EnvUtil.clear()
  }
}

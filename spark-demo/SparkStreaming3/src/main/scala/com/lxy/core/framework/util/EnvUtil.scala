package com.lxy.core.framework.util

import org.apache.spark.SparkContext

/**
 * @author lxy
 * @date 2021/7/7
 */
object EnvUtil {
  /**
   * app->controller->dao-service 都是在一个main主线程中执行的，
   *
   * 所以在主线程中开辟一块内存空间（ThreadLocal）将sc 放到 里面其他地方可以使用、
   *
   * ThreadLocal：可以对线程的内存进行控制，存储数据共享数据
   */

  private val scLocal = new ThreadLocal[SparkContext]()

  def put(sc: SparkContext): Unit = {
    scLocal.set(sc)
  }

  def take(): SparkContext = {
    scLocal.get()
  }

  def clear(): Unit = {
    scLocal.remove()
  }
}

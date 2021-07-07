package com.lxy.core.framework.util

import org.apache.spark.SparkContext

/**
 * @author lxy
 * @date 2021/7/7
 */
object EnvUtil {
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

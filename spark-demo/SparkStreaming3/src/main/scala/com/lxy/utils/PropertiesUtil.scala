package com.lxy.utils

import java.io.InputStreamReader
import java.util.Properties

/**
 * @author lxy
 * @date 2021/7/6
 */
object PropertiesUtil {
  def load(propertiesName: String): Properties = {
    val prop = new Properties()
    prop.load(new
        InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName), "UTF-8"))
    prop
  }

}

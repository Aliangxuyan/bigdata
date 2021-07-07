package com.lxy.core.framework.common

import com.lxy.core.framework.util.EnvUtil

/**
 * @author lxy
 * @date 2021/7/7
 */
trait TDao {

  def readFile(path: String) = {
    EnvUtil.take().textFile(path)
  }
}

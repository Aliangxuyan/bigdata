package com.lxy.core.framework.common

import com.lxy.core.framework.util.EnvUtil

/**
 * @author lxy
 * @date 2021/7/7
 */
trait TDao {

  // 读文件，通用的方法
  def readFile(path: String) = {
    //sc
    EnvUtil.take().textFile(path)
  }
}

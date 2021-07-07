package com.lxy.core.framework.controllor

import com.lxy.core.framework.common.TController
import com.lxy.core.framework.service.WordCountService

/**
 * @author lxy
 * @date 2021/7/7
 *
 * 控制层
 */
class WordCountController extends TController {

  private val wordCountService = new WordCountService()

  // 调度
  def dispatch(): Unit = {
    // TODO 执行业务操作
    val array = wordCountService.dataAnalysis()
    array.foreach(println)
  }
}
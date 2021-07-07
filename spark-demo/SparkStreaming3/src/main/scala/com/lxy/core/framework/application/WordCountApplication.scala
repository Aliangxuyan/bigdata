package com.lxy.core.framework.application

import com.lxy.core.framework.common.TApplication
import com.lxy.core.framework.controllor.WordCountController

/**
 * @author lxy
 * @date 2021/7/7
 */
object WordCountApplication extends App with TApplication{

  // 启动应用程序
  start(){
    val controller = new WordCountController()
    controller.dispatch()
  }

}

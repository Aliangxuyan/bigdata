package com.lxy.core.framework.application

import com.lxy.core.framework.common.TApplication
import com.lxy.core.framework.controllor.WordCountController

/**
 * @author lxy
 * @date 2021/7/7
 *
 * 代码按照三层架构的方式实现：MVC
 *
 */
object WordCountApplication extends App with TApplication{

  // 启动应用程序
  start(){
    // op
    val controller = new WordCountController()
    controller.dispatch()
  }

}

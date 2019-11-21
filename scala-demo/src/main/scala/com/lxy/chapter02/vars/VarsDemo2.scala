package com.lxy.chapter02.vars

/**
  * @author lxy
  * @date 2019-11-15
  */
object VarsDemo2 {
  def main(args: Array[String]): Unit = {
    // 类型推导，这时num 就是int 型
    var num = 10
    // 方式1：使用 ideal 提示
    println(num)
    // 方式2：使用isInstanceOf 判断
    println(num.isInstanceOf[Int])

    // 类型确定后就不能修改，说明scala  是强数据类型语言
//    num = 2.3 错误
  }
}

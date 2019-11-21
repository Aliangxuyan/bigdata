package com.lxy.chapter02.vars

/**
  * @author lxy
  * @date 2019-11-15
  */
object VarsDemo1 {
  def main(args: Array[String]): Unit = {
    // 编译器 动态的（逃逸分析），所以不一定对象放在堆里面
    var vars: Int = 10
    var sal: Double = 10.0
    var name: String = "tom"
    var isPass: Boolean = true
    // 在Scala 中小数默认为double 类型，整数默认为int
    var score: Float = 70.9f
  }
}

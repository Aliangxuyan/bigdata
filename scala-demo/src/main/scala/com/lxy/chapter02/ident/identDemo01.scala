package com.lxy.chapter02.ident

/**
  * @author lxy
  * @date 2019-11-17
  * 标识符
  * 在scala  中int  不是保留子而是预定义标志符，可以使用,但是不建议使用
  * _ 不可以，因为在Scala 中，_ 有很多其他的作用
  */
object identDemo01 {
  def main(args: Array[String]): Unit = {

    // 首字母为标识符（比如 +- */ ），后续字符也需要跟一个操作符，至少一个
    val ++ = "hello";
    println(++)

    var -+ = 90;
    println("res = " + -+)

    // 用反引号也可以 `.....` 包括的任意字符串即使是关键字（39个）都可以

    var `true` = "hell"
    println(`true`)

    val Int = 90.12
    println(s"Int = $Int")

  }
}

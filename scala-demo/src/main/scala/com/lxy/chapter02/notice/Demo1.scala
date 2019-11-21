package com.lxy.chapter02.notice

import scala.io.StdIn

/**
  * @author lxy
  * @date 2019-11-17
  *       不支持三目运算符
  */
object Demo1 {
  def main(args: Array[String]): Unit = {
    val num = if (5 > 4) 5 else 4
    //    val num2 = -5 > 4 ? 5 : 4 错误
    StdIn.readChar(); // 从键盘输入
    scala.io.StdIn
  }
}

package com.lxy.chapter04

import scala.io.StdIn

/**
  * @author lxy
  * @date 2019-11-17
  */
object Demo01 {
  def main(args: Array[String]): Unit = {
    println("请输入年龄")
    val age = StdIn.readInt()
    if (age > 18) {
      println("age > 18")
    }
  }
}

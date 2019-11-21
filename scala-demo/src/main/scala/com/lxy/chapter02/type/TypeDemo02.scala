package com.lxy.chapter02.`type`

/**
  * @author lxy
  * @date 2019-11-15
  */
object TypeDemo02 {
  def main(args: Array[String]): Unit = {
    println(sayHello)
  }

  /**
    * 比如开发中我们有一个方法，就会异常终端，这时可以返回nothing
    * 即当我们nothing  作为返回值，就是明确说明该方法没有正常返回值
    *
    * @return
    */
  def sayHello(): Nothing = {
    throw new Exception("抛出异常")
  }
}

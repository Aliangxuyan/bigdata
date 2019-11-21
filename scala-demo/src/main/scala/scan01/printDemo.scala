package scan01

/**
  * @author lxy
  * @date 2019-11-15
  *
  * 字符串输出的三种方式
  */
object printDemo {
  def main(args: Array[String]): Unit = {
    val width: Int = 10
    val height: Double = 10.01
    val sla: Float = 12.02f
    val name: String = "aaaa"

    // 格式化输出
    printf("name:%s,width:%d,height:%.2f \n",name,width,height)

    // scala  支持使用 $ 输出,编译器会去解析$ 符
    println(s"个人信息如下：name:$name,width:$width,height:$height")

    // {} 明确表示表达式，如果对一个变量做一个简单的运算，用大括号括起来
    println(s"个人信息如下2：name:${name},width:${width + 10},height:${height * 10}")

  }
}

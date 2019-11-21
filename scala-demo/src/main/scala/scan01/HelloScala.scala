package scan01

/**
  * @author lxy
  * @date 2019-06-16
  *       类对象，其中声明的都是类似于java 种静态的方法，可以直接调用
  *       Scala 是一个基于完全面向对象的语言，所以没有静态的关键字，所以需要采用Object 声明
  */
// 类对象
object HelloScala {
  // 类似于java 种的main 方法
  // def 关键字用于声明函数
  // public static void main(String[] args)
  def main(args: Array[String]): Unit = {
//        System.out.println("Hello Scala");
    print("Hello Scala")
  }
}

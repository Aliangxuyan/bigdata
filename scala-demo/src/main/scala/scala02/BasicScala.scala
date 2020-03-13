package scala02

/**
  * @author lxy
  *         2019-12-09
  */
object BasicuntilScala {

  def main(args: Array[String]): Unit = {
    //    val tuple1 = (1, 2, 3, "heiheihei")
    //    println(tuple1._1)
    //    println(tuple1)
    //    for (item <- tuple1.productIterator){
    //      println(item)
    //    }

    for (i <- 1 until 3) {
      print(i + " ")
    }
    println()

    for (i <- 1 to 3 if i % 2 == 0) {
      print(i + " ")
    }

    for (i <- 1 to 3; j <- 1 to 2) {
      print(i * 3 + " ")
    }
    println()

    val for5 = for (i <- 1 to 3) yield i
    println(for5)


  }


  def sum(args: Int*) = {
    var result = 0
    for (arg <- args)
      result += arg
    result
  }

  def factorial(n: Int): Int = {
    if (n <= 0)
      1
    else
      n * factorial(n - 1)
  }

  def f7(content: String) = {
    println("f7")
  }

  def f8(content: String) {
    println(content)
  }

  object Lazy {
    def init(): String = {
      println("init方法执行")
      "嘿嘿嘿，我来了~"
    }

    def main(args: Array[String]): Unit = {
      lazy val msg = init()
      println("lazy方法没有执行")
      println(msg)
    }
  }

  object ExceptionSyllabus {
    def divider(x: Int, y: Int): Float = {
      if (y == 0) throw new Exception("0作为了除数")
      else x / y
    }

    def main(args: Array[String]): Unit = {
      try {
        println(divider(10, 3))
      } catch {
        case ex: Exception => println("捕获了异常：" + ex)
      } finally {}
    }

    //定义
    val arr1 = new Array[Int](10)
    //赋值
    arr1(1) = 7
    // 集合元素采用小括号访问
    // 或：
    //定义
    val arr2 = Array(1, 2)
  }

}

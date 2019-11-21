package scan01

/**
  * @author lxy
  *         文档注释 scaladoc
  */
object Common {
  def main(args: Array[String]): Unit = {
    println("lxy scala")
  }

  /**
    * @deprecated 过期
    * @param a
    * @param b
    * 输入a=10 b=20 return 30
    * @return
    */
  def sum(a: Int, b: Int): Int = {
    return a + b
  }
}

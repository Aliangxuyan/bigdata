package scan01

/**
  * @author lxy
  *  2019-11-15
  */
object lookSourceCode {
  def main(args: Array[String]): Unit = {

    // 查看array的源码
    var arr = new Array[String](10)
    for (item <- arr){
      printf(s"item:$item")
    }
  }
}

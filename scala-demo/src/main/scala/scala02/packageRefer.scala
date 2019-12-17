package scala02

/**
  * @author lxy
  * @date 2019-12-11
  */
object packageRefer {

  //3.1  包/作用域
  class Person{
    val name = "Nick"
    def play(message: String): Unit ={
    }
  }




}

//3.2  包对象
package object people {
  val defaultName = "Nick"
}
package people {

  class Person {
    var name = defaultName // 从包对象拿到的常置
  }
}


package scala02

/**
  * @author lxy
  *  2019-12-11
  */
//四  继承
//class Person {
//  var name = ""
//}

class Employee extends Person {
  var salary = 0.0

  def description = "员工姓名：" + name + " 薪水：" + salary
}


//4.2  重写方法
class Person1 {
  var name = ""

  override def toString = getClass.getName + "[name=" + name + "]"
}

class Employee1 extends Person1 {
  var salary = 0.0

  override def toString = super.toString + "[salary=" + salary + "]"
}

//4.3  类型检查和转换
/**
  * 要测试某个对象是否属于某个给定的类，可以用isInstanceOf方法。用asInstanceOf方法将引用转换为子类的引用。classOf获取对象的类名。
  * 1)  classOf[String]就如同Java的 String.class
  * 2)  obj.isInstanceOf[T]就如同Java的obj instanceof T
  * 3)  obj.asInstanceOf[T]就如同Java的(T)obj
  */

//4.4  受保护的字段和方法

//4.5  超类的构造
/**
  * 类有一个主构器和任意数量的辅助构造器，而每个辅助构造器都必须以对先前定义的辅助构造器或主构造器的调用开始。
  * 子类的辅助构造器最终都会调用主构造器，只有主构造器可以调用超类的构造器。
  * 辅助构造器永远都不可能直接调用超类的构造器。在Scala的构造器中，你不能调用super(params)。
  *
  * @param name
  * @param age
  */
//class Person2(val name: String, val age: Int) {
//  override def toString = getClass.getName + "[name=" + name +
//    ",age=" + age + "]"
//}

class Employee2(name: String, age: Int, val salary: Double) extends Person2(name, age) {
  override def toString = super.toString + "[salary=" + salary + "]"
}


//4.6  覆写字段 子类改写父类或者抽象父类的字段，通过以下方式：
//class Person3(val name:String,var age:Int){
//  println("主构造器已经被调用")
//  val school="五道口职业技术学院"
//  def sleep="8 hours"
//  override def toString="我的学校是：" + school + "我的名字和年龄是：" + name + "," + age
//}
//
//class Person3(name:String, age:Int) extends Person3(name, age){
//  override val school: String = "清华大学"
//}


object ExtendsTest {

  def main(args: Array[String]): Unit = {
    println("Hello".isInstanceOf[String])
    println("Hello".asInstanceOf[String])
    println(classOf[String])
  }


}
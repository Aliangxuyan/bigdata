package scala02

/**
  * @author lxy
  * @date 2019-12-10
  */
class dog {
  private var leg = 4

  def shout(content: String) {
    println(content)
  }

  def currentLeg = leg


}

class Dog2 {
  private var _leg = 4

  def leg = _leg

  def leg_=(newLeg: Int) {
    _leg = newLeg
  }
}

package society {
  package professional {

    class Executive {
      private[professional] var workDetails = null
      private[society] var friends = null
      private[this] var secrets = null

      def help(another: Executive) {
        println(another.workDetails)
        //        println(another.secrets) 报错：访问不到
      }
    }

  }

}

import scala.collection.mutable.ArrayBuffer

//嵌套类
class Network {

  class Member(val name: String) {
    val contacts = new ArrayBuffer[Member]
  }

  private val members = new ArrayBuffer[Member]

  def join(name: String) = {
    val m = new Member(name)
    members += m
    m
  }

  //创建两个局域网
  val chatter1 = new Network
  val chatter2 = new Network

  //Fred 和 Wilma加入局域网1
  val tom = chatter1.join("Tom")
  val jack = chatter1.join("Jack")
  //Barney加入局域网2
  val bob = chatter2.join("Bob")

  //tom将同属于局域网1中的jack添加为联系人
  tom.contacts += jack
  //tom.contacts += bob //这样做是不行的，tom和bob不属于同一个局域网，即，tom和bob不是同一个class Member实例化出来的对象
}

import scala.beans.BeanProperty

class Person {
  @BeanProperty var name: String = _
}


class ClassConstructor(var name: String, private var price: Double) {
  // 类体（主构造方法的实现逻辑）
  def myPrintln = println(name + "," + price)

  println("12323")
}

class ClassConstructor2(val name: String = "", val price: Double = 0) {
  println(name + "," + price)
}

class Person2 {
  private var name = ""
  private var age = 0

  def this(name: String) {
    this()
    this.name = name
  }

  def this(name: String, age: Int) {
    this(name)
    this.age = age
  }

  def description = name + " is " + age + " years old"
}

import scala.collection.mutable.ArrayBuffer

import scala.collection.mutable.ArrayBuffer

// 伴生对象
class Network2 {
  private val members = new ArrayBuffer[Network2.Member]

  def join(name: String) = {
    val m = new Network2.Member(name)
    members += m
    m
  }

  def description = "该局域网中的联系人：" +
    (for (m <- members) yield m.description).mkString(", ")
}

object Network2 {

  class Member(val name: String) {
    val contacts = new ArrayBuffer[Member]

    def description = name + "的联系人：" +
      (for (c <- contacts) yield c.name).mkString(" ")
  }

}

//投影
class Network3 {

  class Member(val name: String) {
    val contacts = new ArrayBuffer[Network3#Member]
  }

  private val members = new ArrayBuffer[Member]

  def join(name: String) = {
    val m = new Member(name)
    members += m
    m
  }
}

//2.1  单例对象 Scala中没有静态方法和静态字段，可以用object这个语法结构来达到同样的目的。
object Dog {
  println("已初始化...")
  private var leg = 0

  def plus() = {
    leg += 1
    leg
  }
}


class Cat {
  val hair = Cat.growHair
  private var name = ""

  def changeName(name: String) = {
    this.name = name
  }

  def describe = println("hair:" + hair + "name:" + name)
}

object Cat {
  private var hair = 0

  private def growHair = {
    hair += 1
    hair
  }
}

// 2.3  Apply方法 1) apply方法一般都声明在伴生类对象中，可以用来实例化伴生类的对象：
class Man private(val sex: String, name: String) {
  def describe = {
    println("Sex:" + sex + "name:" + name)
  }
}

object Man {
  def apply(name: String) = {
    new Man("男", name)
  }
}


// 2)  也可以用来实现单例模式，我们只需要对上述列子稍加改进：
class Man1 private(val sex: String, name: String) {
  def describe = {
    println("Sex:" + sex + "name:" + name)
  }
}

object Man1 {
  var instance: Man1 = null

  def apply(name: String) = {
    if (instance == null) {
      instance = new Man1("男", name)
    }
    instance
  }
}

// 2.4  应用程序对象
object Hello extends App {
  if (args.length > 0)
    println("Hello, " + args(0))
  else
    println("Hello, World!")
}

// 2.5  枚举
object TrafficLightColor extends Enumeration {
  val Red = Value(0, "Stop")
  val Yellow = Value(1, "Slow")
  val Green = Value(2, "Go")
}


object ClassTest {
  def main(args: Array[String]): Unit = {

    println(TrafficLightColor.Red)
    println(TrafficLightColor.Red.id)

    println(TrafficLightColor.Yellow)
    println(TrafficLightColor.Yellow.id)

    println(TrafficLightColor.Green)
    println(TrafficLightColor.Green.id)

//    val man1 = Man1("Nick")
//    val man2 = Man1("Thomas")
//    man1.describe
//    man2.describe

    //    val man1 = Man("Nick")
    //    val man2 = Man("Thomas")
    //    man1.describe
    //    man2.describe

    //    val cat1 = new Cat
    //    val cat2 = new Cat
    //
    //    cat1.changeName("黑猫")
    //    cat2.changeName("白猫")
    //
    //    cat1.describe
    //    cat2.describe

    //    val chatter5 = new Network3
    //    val chatter6 = new Network3
    //
    //    //Fred 和 Wilma加入局域网1
    //    val fred3 = chatter5.join("Fred")
    //    val wilma3 = chatter5.join("Wilma")
    //    //Barney加入局域网2
    //    val barney3 = chatter6.join("Barney")
    //    fred3.contacts += wilma3


    //    val chatter3 = new Network2
    //    val chatter4 = new Network2
    //
    //    //tom 和 jack加入局域网1
    //    val tom2 = chatter3.join("Tom")
    //    val jack2 = chatter3.join("Jack")
    //    //Bob加入局域网2
    //    val bob2 = chatter4.join("Bob")
    //    //Tom将同属于局域网3中的Jack添加为联系人
    //    tom2.contacts += jack2
    //    //Tom将不同属于局域网3中，属于局域网4中的的Bob添加为联系人
    //    tom2.contacts += bob2
    //
    //    println(chatter3.description)
    //    println(chatter4.description)
    //
    //    println(tom2.description)
    //    println(jack2.description)
    //    println(bob2.description)

    //    //创建两个局域网
    //    val chatter1 = new Network
    //    val chatter2 = new Network
    //
    //    //Fred 和 Wilma加入局域网1
    //    val tom = chatter1.join("Tom")
    //    val jack = chatter1.join("Jack")
    //    //Barney加入局域网2
    //    val bob = chatter2.join("Bob")
    //
    //    //tom将同属于局域网1中的jack添加为联系人
    //    tom.contacts += jack
    //tom.contacts += bob //这样做是不行的，tom和bob不属于同一个局域网，即，tom和bob不是同一个class Member实例化出来的对象


    //    val dog = new dog
    //    dog shout "汪汪汪" // dog.shout(“xxxx”)
    //    println(dog currentLeg)
    //
    //    val dog2 = new Dog2
    //    dog2.leg_=(10)
    //    println(dog2.leg)

    //    val person = new Person
    //    person.setName("Nick")
    //    println(person.getName)
    //    println(person.name)

    //    val classConstructor = new ClassConstructor("《傲慢与偏见》", 20.5)
    //    classConstructor.myPrintln

    //    val classConstructor2 = new ClassConstructor2("aa", 20)
    //    val classConstructor2_2 = new ClassConstructor2()
  }
}
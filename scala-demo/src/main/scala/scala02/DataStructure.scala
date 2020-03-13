package scala02

import scala.collection.mutable.ArrayBuffer

/**
  * @author lxy
  *         2019-12-09
  */
object DataStructure {


  // 定长数组（声明泛型）
  def f1() {
    //定义
    val arr1 = new Array[Int](10)
    //赋值
    arr1(1) = 7 // 集合元素采用小括号访问
    // 或：
    //定义
    val arr2 = Array(1, 2)
  }

  //  变长数组（声明泛型）
  def f2() {
    //定义
    val arr2 = ArrayBuffer[Int]()
    //追加值
    arr2.append(7)
    //重新赋值
    arr2(0) = 7

  }

  //  定长数组与变长数组的转换
  // arr1.toBuffer
  //arr2.toArray

  // 多维数组
  def f3() {
    //定义
    val arr3 = Array.ofDim[Double](3, 4)
    //赋值/
    arr3(1)(1) = 11.11
  }

  // 与java 数组的转化
  def f4: Unit = {
    val arr4 = ArrayBuffer("1", "2", "3")
    //Scala to Java
    import scala.collection.JavaConversions.bufferAsJavaList
    val javaArr = new ProcessBuilder(arr4)
    println(javaArr.command())

    // 数组遍历
    for (x <- arr4) {
      println(x)
    }
  }

  def f5(): Unit = {
    import scala.collection.JavaConversions.asScalaBuffer
    import scala.collection.mutable.Buffer
    //    val scalaArr: Buffer[String] = javaArr.command()
    //    println(scalaArr)
  }

  // 元组
  def f6(): Unit = {
    val tuple1 = (1, 2, 3, "heiheihei")
    println(tuple1)

    //  元组数据的访问，注意元组元素的访问有下划线，并且访问下标从1开始，而不是0
    val value1 = tuple1._4
    println(value1)

    //元组的遍历
    for (elem <- tuple1.productIterator) {
      print(elem)
    }
    println()
  }

  // 列表
  def f7() {
    //创建List
    val list1 = List(1, 2, 3, 4)

    // 访问List元素
    val value1 = list1(1)
    println(value1)

    //  List元素的追加
    val list2 = list1 :+ 99
    println(list2)
    val list3 = 100 +: list1
    println(list3)


    //List的创建与追加，符号“::”，注意观察去掉Nil和不去掉Nil的区别
    val list4 = 1 :: 2 :: 3 :: list1 :: Nil
    println(list4)
  }

  // 队列 Queue
  def f8(): Unit = {
    //    1)  队列的创建
    import scala.collection.mutable
    val q1 = new mutable.Queue[Int]
    println(q1)

    //队列元素的追加
    q1 += 1
    println(q1)

    //向队列中追加List
    q1 ++= List(2, 3, 4)
    println(q1)

    // 按照进入队列的顺序删除元素
    q1.dequeue()
    println(q1)

    //塞入数据
    q1.enqueue(9, 8, 7)
    println(q1)

    //返回队列的第一个元素
    println(q1.head)

    //返回队列最后一个元素
    println(q1.last)

    // 返回除了第一个以外剩余的元素
    println(q1.tail)

  }

  // 映射 Map
  def f9(): Unit = {
    //构造不可变映射
    val map1 = Map("Alice" -> 10, "Bob" -> 20, "Kotlin" -> 30)
    //构造可变映射
    val map2 = scala.collection.mutable.Map("Alice" -> 10, "Bob" -> 20, "Kotlin" -> 30)
    //空的映射
    val map3 = new scala.collection.mutable.HashMap[String, Int]
    // 对偶元组
    val map4 = Map(("Alice", 10), ("Bob", 20), ("Kotlin", 30))
    // 取值 如果映射中没有值，则会抛出异常，使用contains方法检查是否存在key。如果通过 映射.get(键) 这样的调用返回一个Option对象，要么是Some，要么是None。
    if (map3.contains("Alice")) {
      val value3 = map3.get("Alice")
      println(value3)
    }
    //    val value1 = map4("Alice") //建议使用get方法得到map中的元素
    val value1 = map3.get("Alice") //建议使用get方法得到map中的元素
    println(value1)

    //更新值
    map2("Alice") = 99
    println(map2("Alice"))
    //    或：
    map2 += ("Bob" -> 99)
    map2 -= ("Alice", "Kotlin")
    println(map2)
    //    或：
    val map5 = map2 + ("AAA" -> 10, "BBB" -> 20)
    println(map5)

    // 遍历
    for ((k, v) <- map1) println(k + " is mapped to " + v)
    for (v <- map1.keys) println(v)
    for (v <- map1.values) println(v)
    for (v <- map1) println(v)
  }

  // 集 Set
  def f10(): Unit = {

    //1)  Set不可变集合的创建
    val set = Set(1, 2, 3)
    println(set)

    //2)  Set可变集合的创建，如果import了可变集合，那么后续使用默认也是可变集合
    import scala.collection.mutable.Set
    val mutableSet = Set(1, 2, 3)

    // 3)  可变集合的元素添加
    mutableSet.add(4)
    mutableSet += 6
    // 注意该方法返回一个新的Set集合，而非在原有的基础上进行添加
    mutableSet.+(5)

    //4)  可变集合的元素删除
    mutableSet -= 1
    mutableSet.remove(2)
    println(mutableSet)

    //5) 遍历
    for (x <- mutableSet) {
      println(x)
    }
  }

  //  集合元素与函数的映射

  def f11(): Unit = {
    //    1) map：将集合中的每一个元素通过指定功能（函数）映射（转换）成新的结果集合
    val names = List("Alice", "Bob", "Nick")
    println(names.map(_.toUpperCase))

    //    2) flatmap：flat即压扁，压平，扁平化，效果就是将集合中的每个元素的子元素映射到某个函数并返回新的集合
    val names2 = List("Alice", "Bob", "Nick")
    println(names2.flatMap(_.toUpperCase()))

  }

  //化简、折叠、扫描
  def f12(): Unit = {
    //1) 折叠，化简：将二元函数引用于集合中的函数
    val list = List(1, 2, 3, 4, 5)
    val i1 = list.reduceLeft(_ - _)
    val i2 = list.reduceRight(_ - _)
    println(i1)
    println(i2)

    val list2 = List(1, 9, 2, 8)
    val i4 = list2.fold(5)((sum, y) => sum + y)
    println(i4)

    val list3 = List(1, 9, 2, 8)
    val i5 = list3.foldRight(100)(_ - _)
    println(i5)

    //注：foldLeft和foldRight有一种缩写方法对应分别是：/:和:\
    val list4 = List(1, 9, 2, 8)
    val i6 = (0 /: list4) (_ - _)
    println(i6)

    //3)  统计一句话中，各个文字出现的次数
    val sentence = "一首现一代诗《笑里藏刀》:哈哈哈哈哈哈哈哈哈哈哈哈哈哈哈哈哈刀哈哈哈哈哈哈哈哈哈哈哈哈哈哈哈哈哈"
    val i7 = (Map[Char, Int]() /: sentence) ((m, c) => m + (c -> (m.getOrElse(c, 0) + 1)))
    println(i7)

    //4)  折叠，化简，扫描
    //    这个理解需要结合上面的知识点，扫描，即对某个集合的所有元素做fold操作，但是会把产生的所有中间结果放置于一个集合中保存。
    val i8 = (1 to 10).scanLeft(0)(_ + _)
    println(i8)

  }

  def f13(): Unit = {
    //1.10  拉链
    val list1 = List("15837312345", "13737312345", "13811332299")
    val list2 = List(17, 87)
    var z1 = list1 zip list2
    println(z1)

    //1.11  迭代器 你可以通过iterator方法从集合获得一个迭代器，通过while循环和for表达式对集合进行遍历。
    val iterator = List(1, 2, 3, 4, 5).iterator
    while (iterator.hasNext) {
      println(iterator.next())
    }
    for (enum <- iterator) {
      println(enum)
    }
    //    或：

  }


  //1.12  流 Stream
  // stream是一个集合。这个集合，可以用于存放，无穷多个元素，但是这无穷个元素并不会一次性生产出来，而是需要用到多大的区间，就会动态的生产，末尾元素遵循lazy规则

  //  使用#::得到一个stream
  def numsForm(n: BigInt): Stream[BigInt] = n #:: numsForm(n + 1)


  def f14(): Unit = {

    //
    def numsForm(n: BigInt): Stream[BigInt] = n #:: numsForm(n + 1)

  }

  //1.13  视图 View
  //  Stream的懒执行行为，你可以对其他集合应用view方法来得到类似的效果，该方法产出一个其方法总是被懒执行的集合。但是view不会缓存数据，每次都要重新计算。
  //  例如：我们找到10万以内，所有数字倒序排列还是它本身的数字。
  def f15(): Unit = {
    val viewSquares = (1 to 100000)
      .view
      .map(x => {
        //        println(x)
        x.toLong * x.toLong
      }).filter(x => {
      x.toString == x.toString.reverse
    })

    println(viewSquares(3))

    for (x <- viewSquares) {
      print(x + "，")
    }
  }


  //
  def f16(): Unit = {
    (1 to 5).foreach(println(_))
    println()
    (1 to 5).par.foreach(println(_))
  }

  def f17(): Unit = {
    val result1 = (0 to 10000).map { case _ => Thread.currentThread.getName }.distinct
    val result2 = (0 to 10000).par.map { case _ => Thread.currentThread.getName }.distinct
    println(result1)
    println(result2)
  }


  //switch 与default等效的是捕获所有的case_ 模式。如果没有模式匹配，抛出MatchError，每个case
  def swith(): Unit = {
    var result = 0;
    val op: Char = '-'

    op match {
      case '+' => result = 1
      case '-' => result = -1
      case _ => result = 0
    }
    println(result)
  }

  def switch2(): Unit = {
    for (ch <- "+-3!") {
      var sign = 0
      var digit = 0

      ch match {
        case '+' => sign = 1
        case '-' => sign = -1
        case _ if ch.toString.equals("3") => digit = 3
        case _ => sign = 0
      }
      println(ch + " " + sign + " " + digit)
    }
  }

  // 类型模式
  def f18: Unit = {
    val a = 4
    val obj = if (a == 1) 1
    else if (a == 2) "2"
    else if (a == 3) BigInt(3)
    else if (a == 4) Map("aa" -> 1)
    else if (a == 5) Map(1 -> "aa")
    else if (a == 6) Array(1, 2, 3)
    else if (a == 7) Array("aa", 1)
    else if (a == 8) Array("aa")
    val r1 = obj match {
      case x: Int => x
      case s: String => s.toInt
      case BigInt => -1 //不能这么匹配
      case _: BigInt => Int.MaxValue
      case m: Map[String, Int] => "Map[String, Int]类型的Map集合"
      case m: Map[_, _] => "Map集合"
      case a: Array[Int] => "It's an Array[Int]"
      case a: Array[String] => "It's an Array[String]"
      case a: Array[_] => "It's an array of something other than Int"
      case _ => 0
    }
    println(r1 + ", " + r1.getClass.getName)
  }

  // 2.5  匹配数组、列表、元组
  /**
    * Array(0) 匹配只有一个元素且为0的数组。
    * Array(x,y) 匹配数组有两个元素，并将两个元素赋值为x和y。
    * Array(0,_*) 匹配数组以0开始。
    */

  def f19(): Unit = {
    // 1)  匹配数组
    for (arr <- Array(Array(0), Array(1, 0), Array(0, 1, 0), Array(1, 1, 0), Array(1, 1, 0, 1))) {
      val result = arr match {
        case Array(0) => "0"
        case Array(x, y) => x + " " + y
        case Array(x, y, z) => x + " " + y + " " + z
        case Array(0, _*) => "0..."
        case _ => "something else"
      }
      println(result)
    }

    // 2)  匹配列表
    for (lst <- Array(List(0), List(1, 0), List(0, 0, 0), List(1, 0, 0))) {
      val result = lst match {
        case 0 :: Nil => "0"
        case x :: y :: Nil => x + " " + y
        case 0 :: tail => "0 ..."
        case _ => "something else"
      }
      println(result)
    }

    //3)  匹配元组
    for (pair <- Array((0, 1), (1, 0), (1, 1))) {
      val result = pair match {
        case (0, _) => "0 ..."
        case (y, 0) => y + " 0"
        case _ => "neither is 0"
      }
      println(result)
    }
  }

  // 变量声明中的模式
  def f20(): Unit = {
    val (x, y) = (1, 2)
    val (q, r) = BigInt(10) /% 3
    val arr = Array(1, 7, 2, 9)
    val Array(first, second, _*) = arr
    println(first, second)

    //2.8  for表达式中的模式
    import scala.collection.JavaConverters._
    for ((k, v) <- System.getProperties.asScala)
      println(k + " -> " + v)

    println("**************")
    for ((k, "") <- System.getProperties.asScala)
      println(k)

    println("**************")
    for ((k, v) <- System.getProperties.asScala if v == "")
      println(k)


  }

  // 2.9  样例类
  def f21() {
    //    1)  样例类的创建
    abstract class Amount
    case class Dollar(value: Double) extends Amount
    case class Currency(value: Double, unit: String) extends Amount
    case object Nothing extends Amount

    //2)  当我们有一个类型为Amount的对象时，我们可以用模式匹配来匹配他的类型，并将属性值绑定到变量：
    for (amt <- Array(Dollar(1000.0), Currency(1000.0, "EUR"), Nothing)) {
      val result = amt match {
        case Dollar(v) => "$" + v
        case Currency(_, u) => u
        case Nothing => ""
      }
      println(amt + ": " + result)
    }

    //2.10  Copy方法和带名参数
    val amt = Currency(29.95, "EUR")
    val price = amt.copy(value = 19.95)
    println(amt)
    println(price)
    println(amt.copy(unit = "CHF"))
  }

  //2.12  匹配嵌套结构
  def f22(): Unit = {
    //1)  创建样例类
    abstract class Item
    case class Article(description: String, price: Double) extends Item
    case class Bundle(description: String, discount: Double, item: Item*) extends Item

    //2)  匹配嵌套结构
    val sale = Bundle("愚人节大甩卖系列", 10,
      Article("《九阴真经》", 40),
      Bundle("从出门一条狗到装备全发光的修炼之路系列", 20,
        Article("《如何快速捡起地上的装备》", 80),
        Article("《名字起得太长躲在树后容易被地方发现》", 30)))

    //3)  将descr绑定到第一个Article的描述
    val result1 = sale match {
      case Bundle(_, _, Article(descr, _), _*) => descr
    }
    println(result1)

    //4)  通过@表示法将嵌套的值绑定到变量。_*绑定剩余Item到rest
    val result2 = sale match {
      case Bundle(_, _, art@Article(_, _), rest@_*) => (art, rest)
    }
    println(result2)

    //    5)  不使用_*绑定剩余Item到rest

    val result3 = sale match {
      case Bundle(_, _, art@Article(_, _), rest) => (art, rest)
    }
    println(result3)

    // 6)  计算某个Item价格的函数，并调用
    def price(it: Item): Double = {
      it match {
        case Article(_, p) => p
        case Bundle(_, disc, its@_*) => its.map(price _).sum - disc
      }
    }

    println(price(sale))


  }


  def main(args: Array[String]): Unit = {
    f22
    //    f7()
    //    f8()
    //    f11()
    //        f12()
    //    f13()
    //    f21
    //    f14()
    //    传递一个值，并打印stream集合
    //    val tenOrMore = numsForm(10)
    //    println(tenOrMore)
    //    //tail的每一次使用，都会动态的向stream集合按照规则生成新的元素
    //    println(tenOrMore.tail)
    //    println(tenOrMore)
    //
    //    //4)  使用map映射stream的元素并进行一些计算
    //    println(numsForm(5).map(x => x * x))
    //    println(numsForm(5).tail.map(x => x * x))
    //    swith

  }
}

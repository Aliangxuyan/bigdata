package com.lxy.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author lxy
 * @date 2021/7/7
 */
object Spark07_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List[Int](1,2))

    val user = new User()

    // SparkException: Task not serializable
    // NotSerializableException: com.atguigu.bigdata.spark.core.rdd.operator.action.Spark07_RDD_Operator_Action$User

    // RDD算子中传递的函数是会包含闭包操作，那么就会进行检测功能
    // 闭包检测
    /**
     *  源码：val cleanF = sc.clean(f) 有检查闭包的操作
     *
     */
    rdd.foreach(
      num => {
        println("age = " + (user.age + num))
        user.age = (user.age + num)
      }
    )
    println("age = " + (user.age ))

    sc.stop()

  }

  //class User extends Serializable {
  // 样例类在编译时，会自动混入序列化特质（实现可序列化接口）
//  case class User() {
  class User {
    var age: Int = 30
  }

}

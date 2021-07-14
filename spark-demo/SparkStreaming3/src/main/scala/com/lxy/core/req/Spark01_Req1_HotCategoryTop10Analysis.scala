package com.lxy.core.req

import org.apache.spark.rdd.RDD

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author lxy
 * @date 2021/7/7
 *
 *       Spark 案例实操
 *       6.1Top10 热门品类
 *
 *       第一种方式：
 *
 *       存在问题：
 *       1）actionRDD 重复使用
 *       2）coGroup 使用可能存在shuffle（因为是不同的数据源，分区数不一致的情况下可能存在shuffle）
 *       if (rdd.partitioner == Some(part)) {
 *       logDebug("Adding one-to-one dependency with " + rdd)
 *       new OneToOneDependency(rdd)
 *       } else {
 *       logDebug("Adding shuffle dependency with " + rdd)
 *       new ShuffleDependency[K, Any, CoGroupCombiner](
 *       rdd.asInstanceOf[RDD[_ <: Product2[K, _]]], part, serializer)
 *       }
 *       }
 */
object Spark01_Req1_HotCategoryTop10Analysis {
  def main(args: Array[String]): Unit = {

    // TODO : Top10热门品类
    val sparConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(sparConf)

    // 1. 读取原始日志数据
    val actionRDD = sc.textFile("datas/user_visit_action.txt")

    // 2. 统计品类的点击数量：（品类ID，点击数量）
    val clickActionRDD = actionRDD.filter(
      action => {
        val datas = action.split("_")
        datas(6) != "-1"
      }
    )

    val clickCountRDD: RDD[(String, Int)] = clickActionRDD.map(
      action => {
        val datas = action.split("_")
        (datas(6), 1)
      }
    ).reduceByKey(_ + _)

    // 3. 统计品类的下单数量：（品类ID，下单数量）
    val orderActionRDD = actionRDD.filter(
      action => {
        val datas = action.split("_")
        datas(8) != "null"
      }
    )

    // orderid => 1,2,3
    // 【(1,1)，(2,1)，(3,1)】
    val orderCountRDD = orderActionRDD.flatMap(
      action => {
        val datas = action.split("_")
        val cid = datas(8)
        val cids = cid.split(",")
        cids.map(id => (id, 1))
      }
    ).reduceByKey(_ + _)

    // 4. 统计品类的支付数量：（品类ID，支付数量）
    val payActionRDD = actionRDD.filter(
      action => {
        val datas = action.split("_")
        datas(10) != "null"
      }
    )

    // orderid => 1,2,3
    // 【(1,1)，(2,1)，(3,1)】
    val payCountRDD = payActionRDD.flatMap(
      action => {
        val datas = action.split("_")
        val cid = datas(10)
        val cids = cid.split(",")
        cids.map(id => (id, 1))
      }
    ).reduceByKey(_ + _)

    // 5. 将品类进行排序，并且取前10名
    //    点击数量排序，下单数量排序，支付数量排序
    //    元组排序：先比较第一个，再比较第二个，再比较第三个，依此类推
    //    ( 品类ID, ( 点击数量, 下单数量, 支付数量 ) )
    // 将不同的 rdd 连接到一起，
    //
    /**
     * 思考：
     * 1) join : 点击的数据有，可能没有下单,所以pass
     * 2）zip: 和连接数量和位置有关系，所以pass
     * 3) leftouterjoin: 不能保证左边的数据一定有，没法确定哪个是左右
     * 4）cogroup :  即使不存在也能关联：cogroup =  connect + group
     *
     */
    //
    //  cogroup = connect + group
    //  ( 品类ID, ( 点击数量, 下单数量, 支付数量 ) )
    val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] =
    clickCountRDD.cogroup(orderCountRDD, payCountRDD)
    val analysisRDD = cogroupRDD.mapValues {
      case (clickIter, orderIter, payIter) => {

        var clickCnt = 0
        val iter1 = clickIter.iterator
        if (iter1.hasNext) {
          clickCnt = iter1.next()
        }
        var orderCnt = 0
        val iter2 = orderIter.iterator
        if (iter2.hasNext) {
          orderCnt = iter2.next()
        }
        var payCnt = 0
        val iter3 = payIter.iterator
        if (iter3.hasNext) {
          payCnt = iter3.next()
        }

        (clickCnt, orderCnt, payCnt)
      }
    }

    val resultRDD = analysisRDD.sortBy(_._2, false).take(10)

    // 6. 将结果采集到控制台打印出来
    resultRDD.foreach(println)

    sc.stop()
  }
}

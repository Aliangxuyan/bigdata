package com.lxy.offline

import breeze.numerics.sqrt
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

case class ProductRating(userId: Int, productId: Int, score: Double, timestamp: Int)

case class MongoConfig(uri: String, db: String)

// 定义标准推荐对象
case class Recommendation(productId: Int, score: Double)

// 定义用户的推荐列表
case class UserRecs(userId: Int, recs: Seq[Recommendation])

// 定义商品相似度列表
case class ProductRecs(productId: Int, recs: Seq[Recommendation])

/**
  * @author lxy
  * @date 2019-12-18
  */
object ALSTaniner {

  def main(args: Array[String]): Unit = {
    val MONGODB_RATING_COLLECTION = "Rating"
    val USER_RECS = "UserRecs"
    val PRODUCT_RECS = "productRacs"
    val USER_MAX_RECOMMONDATION = 20;


    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )
    // 创建一个spark config
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("Offline")
    // 创建spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 加载数据
    val ratingRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating]
      .rdd
      .map(
        rating => Rating(rating.userId, rating.productId, rating.score)
      ).cache()

    // 数据集的切分
    val splits = ratingRDD.randomSplit(Array(0.8, 0.2))
    val traininRDD = splits(0)
    val testingRDD = splits(1)

    // 核心实现，输出最优参数
    adjustALJSParams(testingRDD, testingRDD)

    spark.close()
  }

  def adjustALJSParams(trainData: RDD[Rating], testData: RDD[Rating]): Unit = {
    // 直接遍历数组中定义的参数取值
    val result = for (rank <- Array(5, 10, 20, 50); lambda <- Array(1, 0.1, 0.01))
    // 将结果保存
      yield {
        //训练模型
        val model = ALS.train(trainData, rank, 10, lambda)
        val rmse = getRmse(model, testData)
        (rank, lambda, rmse)
      }

    // 按照rmse 排序并输出最优参数
    println(result.minBy(_._3))
  }

  def getRmse(model: MatrixFactorizationModel, data: RDD[Rating]): Double = {
    //构建 userProducts 得到预测评分矩阵
    val userProducts = data.map(item => (item.user, item.product))
    val predictRating = model.predict(userProducts)

    // 按照公式计算rmse,首先把预测评分和实际评分表（以userID 和 productID）做一个连接按照
    val observed = data.map(item => ((item.user, item.product), item.rating))
    val predict = predictRating.map(item => ((item.user, item.product), item.rating))

    observed.join(predict).map {
      case ((userId, productId), (actual, pre)) =>
        // 误差平方的计算
        val err = actual - pre
        err * err
    }
      // 均值
      .mean()
  }
}

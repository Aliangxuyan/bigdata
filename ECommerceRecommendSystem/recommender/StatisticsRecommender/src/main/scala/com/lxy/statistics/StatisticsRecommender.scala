package com.lxy.statistics

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author lxy
  * @date 2019-12-17
  */


case class Rating(userId: Int, productId: Int, score: Double, timestamp: Int)

case class MongoConfig(uri: String, db: String)

object StatisticsRecommender {

  val MONGODB_RATING_COLLECTION = "Rating"

  //统计的表的名称 
  val RATE_MORE_PRODUCTS = "RateMoreProducts"
  val RATE_MORE_RECENTLY_PRODUCTS = "RateMoreRecentlyProducts"
  val AVERAGE_PRODUCTS = "AverageProducts"


  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[1]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // 创建一个spark config
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommender")

    //创建sparksession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    //加载数据
    val ratingDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    //创建一张叫rating  的临时表
    ratingDF.createOrReplaceTempView("ratings")

    // TODO:用sparksql 去做不同的统计推荐
    //1、历史热门商品，按照评分个数统计
    val rateMoreProductsDF = spark.sql("select productId, count(productId) as count from ratings group by productId order by count desc")

    storeDFInMongoDB(rateMoreProductsDF, RATE_MORE_PRODUCTS)

    //2、近期热门商品，按照评分个数统计（加时间戳）
    // 创建一个日期格式化工具
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    // 注册UDF ，将timestamp 转化为年月格式yyyyMM
    spark.udf.register("changeDate", (x: Int) => simpleDateFormat.format(new Date(x * 1000L)).toInt)

    // 将原始rating 转化为想要的结构 predictorId,score,yearmonth
    val ratingOfYearMonthDF = spark
      .sql("select productId, score, changeDate(timestamp) as yearmonth from ratings")

    // 将新的数据集注册成为一张表
    ratingOfYearMonthDF.createOrReplaceTempView("ratingOfMonth")

    val rateMoreRecentlyProducts = spark
      .sql("select productId, count(productId) as count ,yearmonth from ratingOfMonth group by yearmonth,productId order by yearmonth desc, count desc")

    rateMoreRecentlyProducts
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", RATE_MORE_RECENTLY_PRODUCTS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //3、优质商品，主要看商品的平均评分
    val averageProductsDF = spark.sql("select productId, avg(score) as avg from ratings group by productId order by avg desc")
    storeDFInMongoDB(averageProductsDF, AVERAGE_PRODUCTS)

    spark.stop()


  }

  def storeDFInMongoDB(df: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit = {
    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }


}

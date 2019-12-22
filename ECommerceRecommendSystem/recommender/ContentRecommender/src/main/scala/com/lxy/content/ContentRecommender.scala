package com.lxy.content

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix

/**
  * @author lxy
  * @date 2019-12-22
  */


case class Product(productId: Int, name: String, imageUrl: String, categories: String, tags: String)

case class MongoConfig(uri: String, db: String)

// 定义标准推荐对象
case class Recommendation(productId: Int, score: Double)

// 定义商品相似度列表
case class ProductRecs(productId: Int, recs: Seq[Recommendation])


object ContentRecommender {

  case class Product(productId: Int, name: String, imageUrl: String, categories: String, tags: String)

  case class MongoConfig(uri: String, db: String)

  // 定义标准推荐对象
  case class Recommendation(productId: Int, score: Double)

  // 定义商品相似度列表
  case class ProductRecs(productId: Int, recs: Seq[Recommendation])

  // 定义mongodb中存储的表名
  val MONGODB_PRODUCT_COLLECTION = "Product"
  val CONTENT_PRODUCT_RECS = "ContentBasedProductRecs"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )
    // 创建一个spark config
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ContentRecommender")
    // 创建spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 载入数据，做预处理
    val productTagsDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_PRODUCT_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Product]
      .map(
        x => (x.productId, x.name, x.tags.map(c => if (c == '|') ' ' else c))
      )
      .toDF("productId", "name", "tags")
      .cache()

    // todo ,用tf-idf 提取商品特征向量
    // 实例化一个分词器，用来做分词,默认按照空格分
    val tokenizer = new Tokenizer().setInputCol("tags").setOutputCol("words");
    // 用分词器做转换，得到增加一个新列words df
    val wordsDataDF = tokenizer.transform(productTagsDF)

    // 定义一个hashingTf 工具，计算TF
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures()

    wordsDataDF.show()

    val productFeatures = null

    //    //两两配对商品，计算余弦相似度
    //    val productRecs = productFeatures.cartesian(productFeatures)
    //      .filter {
    //        case (a, b) => a._1 != b._1
    //      }
    //      // 计算余弦相似度
    //      .map {
    //      case (a, b) =>
    //        val simScore = consinSim(a._2, b._2)
    //        (a._1, (b._1, simScore))
    //    }
    //      .filter(_._2._2 > 0.4)
    //      .groupByKey()
    //      .map {
    //        case (productId, recs) =>
    //          ProductRecs(productId, recs.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1, x._2)))
    //      }
    //      .toDF()
    //    productRecs.write
    //      .option("uri", mongoConfig.uri)
    //      .option("collection", CONTENT_PRODUCT_RECS)
    //      .mode("overwrite")
    //      .format("com.mongodb.spark.sql")
    //      .save()

    spark.stop()
  }

  /**
    * 余弦求相似度，向量的重合度
    *
    * @param product1
    * @param product2
    * @return
    */
  def consinSim(product1: DoubleMatrix, product2: DoubleMatrix): Double = {
    // 点乘 dot, l2 范数 norm2
    product1.dot(product2) / (product1.norm2() * product2.norm2())
  }
}

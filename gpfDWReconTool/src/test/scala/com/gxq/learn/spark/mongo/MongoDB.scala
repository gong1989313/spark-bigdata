package com.gxq.learn.spark.mongo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.bson.Document
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig

object MongoDB {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("Mongo")
    val sc = new SparkContext(conf)
    //val sc = new SparkContext()

  /*  val conf = new SparkConf(conf)
      .setMaster("local")
      .setAppName("Mingdao-Score")
      //同时还支持mongo驱动的readPreference配置, 可以只从secondary读取数据
      .set("spark.mongodb.input.uri", "mongodb://192.168.2.13:27017,192.168.2.14:27017,192.168.2.15:27017/inputDB.collectionName")
      .set("spark.mongodb.output.uri", "mongodb://192.168.2.13:27017,192.168.2.14:27017,192.168.2.15:27017/outputDB.collectionName")
*/
    val writeConfig = WriteConfig(Map("spark.mongodb.output.uri" -> "mongodb://192.168.2.13:27017",
      "spark.mongodb.input.database" -> "spark",
      "spark.mongodb.input.collection" -> "test"), Some(WriteConfig(sc)))
    val sparkDocuments = sc.parallelize((1 to 10).map(i => Document.parse(s"{spark: $i}")))
    MongoSpark.save(sparkDocuments, writeConfig)
  }
}
package com.gxq.learn.spark.mongo

import org.apache.spark.sql.SparkSession
import org.bson.Document

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.config.WriteConfig

object MongoReader {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://192.168.2.13:28111,192.168.2.14:28112,192.168.2.15:28113/test.spark")
      .config("spark.mongodb.output.uri", "mongodb://192.168.2.13:28111,192.168.2.14:28112,192.168.2.15:28113/test.spark")
      .getOrCreate()

    val writeConfig = WriteConfig(Map("collection" -> "spark", "writeConcern.w" -> "majority"), Some(WriteConfig(spark.sparkContext)))
    val sparkDocuments = spark.sparkContext.parallelize((1 to 10).map(i => Document.parse(s"{spark: $i}")))

    MongoSpark.save(sparkDocuments, writeConfig)
    val readConfig = ReadConfig(Map("collection" -> "spark", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(spark.sparkContext)))
    val customRdd = MongoSpark.load(spark.sparkContext, readConfig)
    customRdd.toDF().show
    println(customRdd.count)
    println(customRdd.first.toJson)
  }
}
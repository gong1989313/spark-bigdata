package com.gxq.learn.recontool.main

import org.apache.spark.sql.SparkSession
import com.mongodb.spark.MongoSpark
import org.apache.spark.SparkConf
import org.bson.Document
import org.apache.spark.SparkConf
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark._
import com.mongodb.spark.config._
import org.bson.Document

object MongoDB {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://192.168.2.14:27017/test.characters")
      .config("spark.mongodb.output.uri", "mongodb://192.168.2.14:27017/test.characters")
      .getOrCreate()

    var sc = spark.sparkContext

    val docs = """
      {"name": "Bilbo Baggins", "age": 50}
      {"name": "Gandalf", "age": 1000}
      {"name": "Thorin", "age": 195}
      {"name": "Balin", "age": 178}
      {"name": "Kíli", "age": 77}
      {"name": "Dwalin", "age": 169}
      {"name": "Óin", "age": 167}
      {"name": "Glóin", "age": 158}
      {"name": "Fíli", "age": 82}
      {"name": "Bombur"}""".trim.stripMargin.split("[\\r\\n]+").toSeq
    spark.sparkContext.parallelize(docs.map(Document.parse)).saveToMongoDB()

    val readConfig = ReadConfig(Map("collection" -> "Person", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sc)))
    val customRdd = MongoSpark.load(sc, readConfig)

    println(customRdd.count)
    println(customRdd.first.toJson)

    val writeConfig = WriteConfig(Map("collection" -> "spark", "writeConcern.w" -> "majority"), Some(WriteConfig(sc)))
    val sparkDocuments = sc.parallelize((1 to 10).map(i => Document.parse(s"{spark: $i}")))

    MongoSpark.save(sparkDocuments, writeConfig)
  }
}
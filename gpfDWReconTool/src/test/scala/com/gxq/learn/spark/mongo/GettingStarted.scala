package com.gxq.learn.spark.mongo

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import org.bson.Document
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object GettingStarted {

  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession

    val conf = new SparkConf().setMaster("local").setAppName("Mongo")
    val sc = new SparkContext(conf)

    val writeConfig = WriteConfig(Map("collection" -> "spark", "writeConcern.w" -> "majority"), Some(WriteConfig(sc)))
    val sparkDocuments = sc.parallelize((1 to 10).map(i => Document.parse(s"{spark: $i}")))

    MongoSpark.save(sparkDocuments, writeConfig)

  }
}
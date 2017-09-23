package com.gxq.learn.spark.mongo

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import org.bson.Document
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object GettingStarted {

  def main(args: Array[String]): Unit = {
        import org.apache.spark.sql.SparkSession

    val spark = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://192.168.2.13:28111,192.168.2.14:28112,192.168.2.15:28113/test.myCollection")
      .config("spark.mongodb.output.uri", "mongodb://192.168.2.13:28111,192.168.2.14:28112,192.168.2.15:28113/test.myCollection")
      .getOrCreate()
    import org.apache.spark.sql.SparkSession

    val writeConfig = WriteConfig(Map("collection" -> "spark", "writeConcern.w" -> "majority"), Some(WriteConfig(spark.sparkContext)))
    val sparkDocuments = spark.sparkContext.parallelize((1 to 10).map(i => Document.parse(s"{spark: $i}")))

    MongoSpark.save(sparkDocuments, writeConfig)

  }
}
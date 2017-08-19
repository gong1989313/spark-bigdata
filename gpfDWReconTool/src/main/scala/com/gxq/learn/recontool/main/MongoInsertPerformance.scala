package com.gxq.learn.recontool.main

import org.apache.spark.sql.SparkSession

import com.mongodb.spark.config.WriteConfig
import com.mongodb.spark.MongoSpark

object MongoInsertPerformance {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("MongoInsert")
      .config("spark.mongodb.input.uri", "mongodb://192.168.2.11:27017/bigdata.FactCommonTransaction")
      .config("spark.mongodb.output.uri", "mongodb://192.168.2.11:27017/bigdata.FactCommonTransaction")
      .config("spark.mongodb.database", "bigdata")
      .getOrCreate()

    val sqlString = """(select
      |p.Id,
      |p.name,
      |p.age,
      |p.address
      |from person
        ) tmp """.trim().stripMargin

    println(sqlString)

    val jdbcDF = spark.read.format("jdbc").option("uri", "jdbc:sqlserver://xxx:2431")
      .option("dbtable", sqlString)
      .option("user", "")
      .option("password", "").load()

    jdbcDF.show()

    val wc = WriteConfig((Map("collection" -> "FactCommontransaction")), Some(WriteConfig(spark)))
    MongoSpark.save(jdbcDF.write.options(wc.asOptions).mode("overwrite"))

  }
}
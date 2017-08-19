package com.gxq.learn.recontool.main

import org.apache.spark.sql.SparkSession

import com.mongodb.spark.config.WriteConfig
import com.mongodb.spark.MongoSpark
import com.gxq.learn.recontool.utils.SparkContextFactory

object MongoInsertPerformance {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("MongoInsert")
      .config("spark.mongodb.input.uri", "mongodb://192.168.2.14:27017/bigdata.Person")
      .config("spark.mongodb.output.uri", "mongodb://192.168.2.14:27017/bigdata.Person")
      .config("spark.mongodb.database", "bigdata")
      .getOrCreate()

    val sqlString = """(select
      |p.id,
      |p.name,
      |p.age,
      |p.address
      |from Person p
        ) tmp """.trim().stripMargin

    println(sqlString)

    /*val jdbcDF = spark.read.format("jdbc").option("uri", "jdbc:sqlserver://xxx:2431")
      .option("dbtable", sqlString)
      .option("user", "")
      .option("password", "").load()

    jdbcDF.show()

    
    MongoSpark.save(jdbcDF.write.options(wc.asOptions).mode("overwrite"))
*/
    val wc = WriteConfig((Map("collection" -> "Person")), Some(WriteConfig(spark)))
  }
}
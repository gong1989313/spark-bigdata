package com.gxq.learn.spark.transformation

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
import com.gxq.learn.recontool.utils.SparkContextFactory
import scala.collection.Seq

object SchemaTest {
  def main(args: Array[String]): Unit = {
    val (sc, ss) = SparkContextFactory.getSparkContext("local")
    val data = sc.parallelize(Seq("Bern;10;12")) // mock for real data

    val schema = new StructType()
    .add("city", StringType, true)
    .add("female", IntegerType, true)
    .add("male", IntegerType, true)

    val cities = data.map(line => {
      val Array(city, female, male) = line.split(";")
      Row(
        city,
        female.toInt,
        male.toInt)
    })

    val citiesDF = ss.createDataFrame(cities, schema)
    citiesDF.show
  }
}
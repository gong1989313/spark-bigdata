package com.gxq.learn.spark.transformation

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SparkTransaform {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("appName").setMaster("local"))
    val rdd = sc.parallelize(List(('a', 1), ('b', 1)))
  }
}
package com.gxq.learn.spark.streaming

import org.apache.spark._
import org.apache.spark.streaming._

object SparkStreaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sparkStreamingDemo").setMaster("local")
    val ssc = new StreamingContext(conf, Seconds(1))
  }
}
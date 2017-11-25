package com.gxq.learn.demo.helloworld

import org.apache.spark.{SparkContext, SparkConf}

object WordCountSample {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("D:/develop/workspace/scala/spark-bigdata/gpfDWLogMonitor/src/test/resources/README.md")   // 读取本地文件

    val words = lines.flatMap(_.split(" ")).filter(word => word != " ")  

    val pairs = words.map(word => (word, 1))  

    val wordscount = pairs.reduceByKey(_ + _)  

    wordscount.collect.foreach(println)  

    sc.stop()   

  }
}
package com.gxq.learn.spark.transformation

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object FoldDemo {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("Fold")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(Array(("a", 1), ("b", 2), ("a", 2), ("c", 5), ("a", 3)), 2)
    val foldRDD = rdd.fold(("d", 0))((val1, val2) => { if (val1._2 >= val2._2) val1 else val2
    })
    println(foldRDD)
  }
}
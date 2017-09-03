package com.gxq.learn.spark.transformation

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object MapPartitions {
//定义函数
  def partitionsFun(/*index : Int,*/iter : Iterator[(String,String)]) : Iterator[String] = {
    var woman = List[String]()
    while (iter.hasNext){
      val next = iter.next()
      next match {
        case (_,"female") => woman = /*"["+index+"]"+*/next._1 :: woman
        case _ =>
      }
    }
    return  woman.iterator
  }
 
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("mappartitions")
    val sc = new SparkContext(conf)
    val l = List(("kpop","female"),("zorro","male"),("mobin","male"),("lucy","female"))
    val rdd = sc.parallelize(l,2)
    val mp = rdd.mapPartitions(partitionsFun)
    /*val mp = rdd.mapPartitionsWithIndex(partitionsFun)*/
    mp.collect.foreach(x => (print(x +" ")))   //将分区中的元素转换成Aarray再输出
  }
}
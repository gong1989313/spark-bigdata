

package com.gxq.learn.spark.transformation

import scala.collection.Seq

object SeqTest {
  def main(args: Array[String]): Unit = {
    var seq = Seq[Any]()
      seq = seq :+ 1
      seq = seq :+ 2
      seq = seq :+ 3
      seq = seq :+ 4
      seq = seq :+ 5
      seq = seq :+ 6
      seq = seq :+ 7
      println(seq)
  }
}
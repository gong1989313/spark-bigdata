package com.gxq.learn

import scala.collection.mutable.ArrayBuffer

object ForTest {
  def main(args: Array[String]): Unit = {
    val buf = new ArrayBuffer[String]
    buf.+=("aaa")
    buf.+=("bbb")
    buf.+=("ccc")
    buf.+=("ddd")
    buf.+=("aaa")

    for(n <- buf.length to 2 by -3){
      println(n)
    }
  }
}
package com.gxq.learn

object ForComputer extends App {
  var pos = 15
  var numUsable = 10
  pos = (pos+1) % numUsable
  println(pos)
}
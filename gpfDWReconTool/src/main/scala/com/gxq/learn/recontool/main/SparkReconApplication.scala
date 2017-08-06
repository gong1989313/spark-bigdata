package com.gxq.learn.recontool.main

import com.gxq.learn.recontool.core.ReconCompute

object SparkReconApplication {
  def main(args: Array[String]): Unit = {
  ReconCompute.invokeRecon("local", "D:/ReconToolTSchema.json")
  }
}
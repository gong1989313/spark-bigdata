package com.gxq.learn.recontool.main

import com.gxq.learn.recontool.core.ReconCompute

object SparkReconApplication {
  def main(args: Array[String]): Unit = {
    val runType = "local"
    val reconType = "FF"
    val reconSchemaPath: String = "D:/ReconToolTSchema.json"
    val leftTableFilePath: String = "D:/BreaksForFII_PRO.csv"
    val rightTableFilePath: String = "D:/BreaksForFII_UAT.csv"
    val excelPath: String = "D:/ReconResult.xlsx"
    val csvPath: String = "D:/hadoop"
    val rowsCut: String = "10000"
    ReconCompute.invokeReconFF(runType, reconType, reconSchemaPath, leftTableFilePath, rightTableFilePath, excelPath, csvPath, rowsCut)
  }
}
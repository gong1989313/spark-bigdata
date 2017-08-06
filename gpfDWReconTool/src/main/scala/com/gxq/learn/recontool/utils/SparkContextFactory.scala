package com.gxq.learn.recontool.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

object SparkContextFactory {
  private[this] val appName = "SparkReconApplication"

  private[this] def getSparkConf = new SparkConf().setAppName(appName)
  private[this] def getSparkSession = SparkSession.builder().appName(appName).getOrCreate()

  def getSparkContext(localOrYarn: String): (SparkContext, SparkSession) = {
    localOrYarn match {
      case "yarn" => {
        (new SparkContext(getSparkConf), getSparkSession)
      }
      case _ => {
        System.setProperty("env", "UAT")
        (new SparkContext(getSparkConf.setMaster("local")), getSparkSession)
      }
    }
  }
}
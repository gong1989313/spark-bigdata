package com.gxq.learn.recontool.utils

import org.apache.spark.sql.SparkSession

object SparkJDBCConnection {
  private[this] val OLAP_UAT_URL = "jdbc:sqlserver://PFOLAPUAT.nam.nsroot.net:2431"
  private[this] val OLAP_UAT_USR = "pfolap_bch"
  private[this] val OLAP_UAT_PWD = "gpfsoa123"

  private[this] val OLAP_PRO_URL = "jdbc:sqlserver://PFOLAPPRO.nam.nsroot.net:2431"
  private[this] val OLAP_PRO_USR = "pfolap_bch"
  private[this] val OLAP_PRO_PWD = "gpfsoa123"

  def loadDataFromJDBC(DBType: String, SQLText: String, session: SparkSession, tableName: String): Unit = {
    Option (DBType) match {
      case Some("UAT") => session.read.format("jdbc").option("url", OLAP_UAT_URL).option("dbtable", "(" + SQLText + ") tmp").option("user", OLAP_UAT_USR).option("password", OLAP_UAT_PWD).load().createTempView(tableName)
      case Some("PRO") => session.read.format("jdbc").option("url", OLAP_PRO_URL).option("dbtable", "(" + SQLText + ") tmp").option("user", OLAP_PRO_USR).option("password", OLAP_PRO_PWD).load().createTempView(tableName)
      case None        => session.read.format("jdbc").option("url", OLAP_UAT_URL).option("dbtable", "(" + SQLText + ") tmp").option("user", OLAP_UAT_USR).option("password", OLAP_UAT_PWD).load().createTempView(tableName)
    }
  }
}
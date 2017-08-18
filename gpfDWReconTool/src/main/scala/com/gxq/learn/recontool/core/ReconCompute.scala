package com.gxq.learn.recontool.core

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.Dataset

import com.gxq.learn.recontool.entity.ReconToolTSchema
import com.gxq.learn.recontool.utils.SparkContextFactory
import com.gxq.learn.recontool.utils.SparkSQLBuilder
import com.gxq.learn.recontool.utils.TSchemaUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import scala.collection.mutable.ArrayBuffer
import com.gxq.learn.recontool.utils.Csv2ExcelUtil
import com.gxq.learn.recontool.utils.ReconUtil
import com.gxq.learn.recontool.utils.SparkJDBCConnection

object ReconCompute {
  def invokeReconFF(runType: String = "yarn",
                    reconType: String = "FF",
                    reconSchemaPath: String = "/sharedata/recontool/ReconToolTSchema.json",
                    leftTableFilePath: String = "/sharedata/recontool/LeftTable.csv",
                    rightTableFilePath: String = "/sharedata/recontool/rigthTable.csv",
                    excelPath: String = "/sharedata/recontool/ReconResult.xlsx",
                    csvPath: String = "hdfs://bigdata/usr/gpfadm",
                    rowsCut: String = "10000") {
    // valiate file path
    ReconUtil.validateFilePath(reconSchemaPath)
    ReconUtil.validateFilePath(leftTableFilePath)
    ReconUtil.validateFilePath(rightTableFilePath)

    val (sc, ss) = SparkContextFactory.getSparkContext(runType)

    // get table schema
    val reconSchema: ReconToolTSchema = TSchemaUtil.getTSchema(sc, reconSchemaPath)
    val keysArray = reconSchema.keys
    val schemasMap = reconSchema.schemas
    val thresholdsMap = reconSchema.thresholds

    // load tables
    val leftTableRDD = sc.textFile(leftTableFilePath)
    val rightTableRDD = sc.textFile(rightTableFilePath)

    // get title colomn list
    val titleArray = ReconUtil.keyPrioritized(keysArray, leftTableRDD.first().split(",", -1).toArray)
    val titleSchema = ReconUtil.getTitleSchema(schemasMap, titleArray)

    // create left recon table
    ReconUtil.createDataFrame(titleSchema, leftTableRDD, titleArray, schemasMap, ss, "ReconTableLeft")

    // create right recon talbe
    ReconUtil.createDataFrame(titleSchema, rightTableRDD, titleArray, schemasMap, ss, "ReconTableRight")

    // generate only one side data sql
    val tableOneOnly = SparkSQLBuilder.buildOnsideSQL("ReconTableLeft", "ReconTableRight", keysArray)
    val tableTwoOnly = SparkSQLBuilder.buildOnsideSQL("ReconTableRight", "ReconTableLeft", keysArray)

    val leftNotInRTDF = ss.sql(tableOneOnly)
    val rightNotInRTDF = ss.sql(tableTwoOnly)

    // generate left or right dataframe
    val lTNotInRTDF = leftNotInRTDF.repartition(1)
    lTNotInRTDF.cache()

    val rTNotInRTDF = rightNotInRTDF.repartition(1)
    rTNotInRTDF.cache()

    // generate left or right dataframe head
    val lTNotInRTTitle = lTNotInRTDF.schema.fields.map(_.name)
    val rTNotInRTTitle = rTNotInRTDF.schema.fields.map(_.name)

    // remove key columns from the schema
    val titleBuffer = titleArray.toBuffer
    for (key <- keysArray) {
      titleBuffer -= key
    }

    // combine keymatch but value not match sql
    val keyMatchOne = SparkSQLBuilder.buildNotMatchSQL("ReconTableLeft", "ReconTableRight", titleBuffer, keysArray, true)
    val keyMatchTwo = SparkSQLBuilder.buildNotMatchSQL("ReconTableLeft", "ReconTableRight", titleBuffer, keysArray, true)

    val leftTable = ss.sql(keyMatchOne)
    val rightTable = ss.sql(keyMatchTwo)

    val sqlText = SparkSQLBuilder.buildSQL(titleArray, keysArray, schemasMap, thresholdsMap, "ReconTableLeft", "ReconTableRight")

    // generate key match date not match dataframe
    val lTInRTDF = ss.sql(sqlText).repartition(1)
    lTInRTDF.cache()
    println("5555555555555555555555")
    lTInRTDF.show()

    // generate key match data not match head array
    val arrBuffer = ArrayBuffer[String]()
    lTInRTDF.schema.fields.foreach(sf => arrBuffer += sf.name)
    val lTInRTHead = arrBuffer.toArray[String]

    // generate csv
    lTInRTDF.write.option("head", "true").mode("overwrite").csv(csvPath)

    Csv2ExcelUtil.writeExcel(
      ReconUtil.getFileName(leftTableFilePath),
      ReconUtil.getFileName(rightTableFilePath),
      lTInRTHead,
      lTInRTDF.rdd.collect(),
      lTNotInRTTitle,
      lTNotInRTDF.rdd.collect(),
      rTNotInRTTitle,
      rTNotInRTDF.rdd.collect(),
      excelPath,
      rowsCut.toInt, keysArray.length)

    lTInRTDF.unpersist()
    lTNotInRTDF.unpersist()
    rTNotInRTDF.unpersist()

    sc.stop()
    ss.stop()
  }

  def invokeReconTT(runType: String = "yarn",
                    reconType: String = "FF",
                    reconSchemaPath: String = "/sharedata/recontool/ReconToolTSchema.json",
                    leftTableFilePath: String = "/sharedata/recontool/LeftTable.csv",
                    rightTableFilePath: String = "/sharedata/recontool/rigthTable.csv",
                    excelPath: String = "/sharedata/recontool/ReconResult.xlsx",
                    csvPath: String = "hdfs://bigdata/usr/gpfadm",
                    rowsCut: String = "10000") {
    // valiate file path
    ReconUtil.validateFilePath(reconSchemaPath)
    ReconUtil.validateFilePath(leftTableFilePath)
    ReconUtil.validateFilePath(rightTableFilePath)

    val (sc, ss) = SparkContextFactory.getSparkContext(runType)

    // get table schema
    val reconSchema: ReconToolTSchema = TSchemaUtil.getTSchema(sc, reconSchemaPath)
    val keysArray = reconSchema.keys
    val schemasMap = reconSchema.schemas
    val thresholdsMap = reconSchema.thresholds

    // load tables
    //val leftTableRDD = sc.textFile(leftTableFilePath)
    //val rightTableRDD = sc.textFile(rightTableFilePath)
    SparkJDBCConnection.loadDataFromJDBC("leftSQLDBType", "leftSQL", ss, "tableName")

    // get title colomn list

    sc.stop()
    ss.stop()
  }
}
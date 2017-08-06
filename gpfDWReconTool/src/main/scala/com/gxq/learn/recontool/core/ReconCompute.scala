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

object ReconCompute {
  def invokeRecon(runType: String, reconSchemaPath: String) {
    val (sc, ss) = SparkContextFactory.getSparkContext(runType)

    val reconSchema: ReconToolTSchema = TSchemaUtil.getTSchema(sc, reconSchemaPath)
    val keysArray = reconSchema.keys
    val schemasMap = reconSchema.schemas
    val thresholdsMap = reconSchema.thresholds

    val leftTableRDD = sc.textFile("D:/BreaksForFII_PRO.csv")
    val rightTableRDD = sc.textFile("D:/BreaksForFII_UAT.csv")
    val titleArray = leftTableRDD.first().split(",", -1).toArray

    val titleSchema = this.getTitleSchema(schemasMap, titleArray)

    this.createDataFrame(titleSchema, leftTableRDD, titleArray, schemasMap, ss, "ReconTableLeft")
    this.createDataFrame(titleSchema, rightTableRDD, titleArray, schemasMap, ss, "ReconTableRight")

    val sqlText = SparkSQLBuilder.buildSQL(titleArray, keysArray, schemasMap, thresholdsMap, "ReconTableLeft", "ReconTableRight")
    println("sqlText:" + sqlText)

    val lTInRTDF = ss.sql(sqlText).repartition(1)
    lTInRTDF.cache()
    println("555555555555555555555555")
    lTInRTDF.show()

    val csvPath = "hdfs://bigdata/usr/gpfadm"

    lTInRTDF.write.option("head", "true").mode("overwrite").csv(csvPath)
  }

  private def getTitleSchema(schemasMap: Map[String, String], titleArray: Array[String]): StructType = {
    val sFields = new Array[StructField](titleArray.length)
    for (i <- 0 until titleArray.length) {
      val dateTypeOption = schemasMap.get(titleArray(i))
      dateTypeOption match {
        case Some("Int")    => sFields(i) = StructField(titleArray(i), IntegerType, true, Metadata.empty)
        case Some("Double") => sFields(i) = StructField(titleArray(i), DoubleType, true, Metadata.empty)
        case None           => sFields(i) = StructField(titleArray(i), StringType, true, Metadata.empty)
        case _              => throw new IllegalArgumentException(dateTypeOption.toString() + " is not support.")
      }
    }
    val titleSchema = new StructType(sFields)
    titleSchema
  }

  private def rddDataConvert(titleArray: Array[String], schemasMap: Map[String, String], sourceRDD: RDD[String]): RDD[Row] = {
    val dataRDD = sourceRDD.map(_.split(",")).map(p => {
      var seq = Seq[Any]()
      for (i <- 0 until p.length) {
        val dateType = schemasMap.get(titleArray(i))
        dateType match {
          case Some("Int")    => { seq = seq :+ p(i).toInt }
          case Some("Double") => { seq = seq :+ p(i).toDouble }
          case None           => { seq = seq :+ p(i) }
          case _              => { seq = seq :+ p(i) }
        }
      }
      Row.fromSeq(seq)
    })
    dataRDD
  }

  private def createDataFrame(titleSchema: StructType, tableRDD: RDD[String], titleArray: Array[String], schemasMap: Map[String, String], ss: SparkSession, viewName: String): Dataset[Row] = {
    val tableFirst = tableRDD.first();
    val filterRDD = tableRDD.filter(_ != tableFirst)
    val tableDataRDD = this.rddDataConvert(titleArray, schemasMap, filterRDD)
    val tableDataDF = ss.createDataFrame(tableDataRDD, titleSchema)
    tableDataDF.createTempView(viewName)
    tableDataDF.show()
    tableDataDF
  }

  private def typeConvert(field: StructField) = {
    field.dataType match {
      case IntegerType => "INTEGER"
      case LongType    => "BIGINT"
      case DoubleType  => "DOUBLE PRECISION"
      case FloatType   => "REAL"
      case ShortType   => "INTEGER"
      case ByteType    => "SMALLINT" // Redshift does not support the BYTE type.
      case BooleanType => "BOOLEAN"
      case StringType =>
        if (field.metadata.contains("maxlength")) {
          s"VARCHAR(${field.metadata.getLong("maxlength")})"
        } else {
          "TEXT"
        }
      case TimestampType  => "TIMESTAMP"
      case DateType       => "DATE"
      case t: DecimalType => s"DECIMAL(${t.precision},${t.scale})"
      case _              => throw new IllegalArgumentException(s"Don't know how to save $field to JDBC")
    }
  }

  private def getFileName(filePath: String) = {
    filePath.substring(filePath.lastIndexOf("/") + 1, filePath.lastIndexOf("."))
  }
}
package com.gxq.learn.recontool.utils

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object ReconUtil {
  def typeConvert(field: StructField) = {
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

  def getFileName(filePath: String) = {
    filePath.substring(filePath.lastIndexOf("/") + 1, filePath.lastIndexOf("."))
  }

  def validateFilePath(filePath: String) = {
    Option(filePath) match {
      case Some(p) if (p.contains('.')) => p
      case None                         => throw new IllegalArgumentException("file path is empty, Please check...")
      case _                            => throw new IllegalArgumentException("file doesn't have extension Name(***.csv or ***.json). Please check...")
    }
  }

  def getTitleSchema(schemasMap: Map[String, String], titleArray: Array[String]): StructType = {
    val sFields = new Array[StructField](titleArray.length)
    for (i <- 0 until titleArray.length) {
      val dateTypeOption = schemasMap.get(titleArray(i))
      dateTypeOption match {
        case Some("Byte")       => sFields(i) = StructField(titleArray(i), ByteType, true, Metadata.empty)
        case Some("Short")      => sFields(i) = StructField(titleArray(i), ShortType, true, Metadata.empty)
        case Some("Int")        => sFields(i) = StructField(titleArray(i), IntegerType, true, Metadata.empty)
        case Some("Long")       => sFields(i) = StructField(titleArray(i), LongType, true, Metadata.empty)
        case Some("Float")      => sFields(i) = StructField(titleArray(i), FloatType, true, Metadata.empty)
        case Some("Double")     => sFields(i) = StructField(titleArray(i), DoubleType, true, Metadata.empty)
        case Some("Boolean")    => sFields(i) = StructField(titleArray(i), BooleanType, true, Metadata.empty)
        case Some("Timestamp")  => sFields(i) = StructField(titleArray(i), TimestampType, true, Metadata.empty)
        case Some("Date")       => sFields(i) = StructField(titleArray(i), DateType, true, Metadata.empty)
        case Some("BigDecimal") => sFields(i) = StructField(titleArray(i), DoubleType, true, Metadata.empty)
        case None               => sFields(i) = StructField(titleArray(i), StringType, true, Metadata.empty)
        case _                  => throw new IllegalArgumentException(dateTypeOption.toString() + " is not support.")
      }
    }
    val titleSchema = new StructType(sFields)
    titleSchema
  }

  private def rddDataConvert(titleArray: Array[String], schemasMap: Map[String, String], sourceRDD: RDD[String]): RDD[Row] = {
    sourceRDD.map(_.split(",", -1)).map(p => for (i <- 0 until p.length) yield {
      val dateType = schemasMap.get(titleArray(i))
      dateType match {
        case Some("Byte")       => { p(i).trim().toByte }
        case Some("Short")      => { this.processNull(p(i).trim()).toShort }
        case Some("Int")        => { this.processNull(p(i).trim()).toInt }
        case Some("Long")       => { this.processNull(p(i).trim()).toLong }
        case Some("Float")      => { this.processNull(p(i).trim()).toFloat }
        case Some("Double")     => { this.processNull(p(i).trim()).toDouble }
        case Some("Boolean")    => { this.processNull(p(i).trim()).toBoolean }
        case Some("BigDecimal") => { this.processNull(p(i).trim()).toDouble }
        case None               => { p(i).trim }
        case _                  => { p(i).trim }
      }
    }).map(Row(_: _*))
  }

  private def processNull(value: String): String = {
    var temp = value
    if (temp == "") {
      temp = "0"
    }
    temp
  }

  def createDataFrame(titleSchema: StructType, tableRDD: RDD[String], titleArray: Array[String], schemasMap: Map[String, String], ss: SparkSession, viewName: String): Dataset[Row] = {
    val tableFirst = tableRDD.first();
    val filterRDD = tableRDD.filter(_ != tableFirst)
    val tableDataRDD = this.rddDataConvert(titleArray, schemasMap, filterRDD)
    val tableDataDF = ss.createDataFrame(tableDataRDD, titleSchema)
    tableDataDF.createTempView(viewName)
    //tableDataDF.show()
    tableDataDF
  }
}
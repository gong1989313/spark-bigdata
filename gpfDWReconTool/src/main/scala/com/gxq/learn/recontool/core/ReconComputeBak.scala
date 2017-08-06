package com.gxq.learn.recontool.core

import org.apache.spark.sql.Row
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

import com.gxq.learn.recontool.entity.ReconToolTSchema
import com.gxq.learn.recontool.utils.SparkContextFactory
import com.gxq.learn.recontool.utils.TSchemaUtil

object ReconComputeBak {
  def invokeRecon(runType: String, reconSchemaPath: String) {
    val (sc, ss) = SparkContextFactory.getSparkContext(runType)

    val reconSchema: ReconToolTSchema = TSchemaUtil.getTSchema(sc, reconSchemaPath)
    val keysArray = reconSchema.keys
    val schemasMap = reconSchema.schemas
    val thresholdsMap = reconSchema.thresholds

    val leftTableRDD = sc.textFile("D:/BreaksForFII_PRO.csv")
    val titleArray = leftTableRDD.first().split(",", -1).toArray
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
    titleSchema.foreach(t => println(t.name))
    val leftTableFirst = leftTableRDD.first()
    val filerRDD = leftTableRDD.filter(_ != leftTableFirst)

    val leftTableDataRDD = filerRDD.map(_.split(",")).map(p => {
      var seq = Seq[Any]()
      for (i <- 0 until p.length) {
        val dateType = schemasMap.get(titleArray(i))
        dateType match {
          case Some("Int")    => { seq = seq :+ p(i).toInt }
          case Some("Double") => { seq = seq :+ p(i).toDouble }
          case None           => { seq = seq :+ p(i) }
        }
      }
      Row.fromSeq(seq)
    })
    val leftTableDataDF = ss.createDataFrame(leftTableDataRDD, titleSchema)
    leftTableDataDF.createTempView("ReconTableLeft")
    leftTableDataDF.show()

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
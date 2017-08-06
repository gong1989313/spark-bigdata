package com.gxq.learn.recontool.utils

import com.gxq.learn.recontool.entity.ReconToolTSchema
import org.apache.spark.SparkContext
import play.api.libs.json.Json
import play.api.libs.json.JsValue
import play.api.libs.json.JsResult
import play.api.libs.json.JsSuccess
import play.api.libs.json.JsPath
import play.api.libs.json.JsError
import scala.collection.SortedMap

object TSchemaUtil {
  def getTSchema(sc: SparkContext, fileUrl: String): ReconToolTSchema = {
    val jsonString = sc.textFile(fileUrl).collect().mkString
    implicit val rtReads = Json.reads[ReconToolTSchema]
    val jsonValue: JsValue = Json.parse(jsonString)
    val rtFromJson: JsResult[ReconToolTSchema] = Json.fromJson[ReconToolTSchema](jsonValue)
    val result = rtFromJson match {
      case JsSuccess(rt: ReconToolTSchema, path: JsPath) => rt
      case e: JsError                                    => throw new IllegalArgumentException(JsError.toJson(e).toString())
    }
    result
  }

  def getHeaderIndexMap(header: String): SortedMap[Int, String] = {
    var headMap = SortedMap[Int, String]()
    val hArr = header.split(",", -1)
    for (i <- 0 until hArr.length) {
      headMap = headMap + (i -> hArr(i).trim())
    }
    headMap
  }
}
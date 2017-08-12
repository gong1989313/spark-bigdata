package com.gxq.learn.recontool.utils

import scala.collection.mutable.Buffer

object SparkSQLBuilder {
  def buildSQL(titleList: Array[String], keyArrs: Array[String], schemasMap: Map[String, String], thresholdsMap: Map[String, String], leftTable: String, rightTable: String): String = {
    (this.buildSelection(titleList, schemasMap, thresholdsMap, leftTable, rightTable).append(this.buildJoin(keyArrs))).mkString
  }

  def buildOnsideSQL(tableOne: String, tableTwo: String, keysArray: Array[String]): String = {
    var tableOnly = s"select a.* from ${tableOne} a left join ${tableTwo} b on 1=1 "
    var keyJoinSQL = ""
    var isNullSQL = ""
    for (key <- keysArray) {
      keyJoinSQL = keyJoinSQL + " and a. " + key + "=b." + key + ""
      isNullSQL = isNullSQL + " and b." + key + " is null "
    }
    tableOnly = tableOnly + keyJoinSQL + " where 1=1 " + isNullSQL
    tableOnly
  }

  private def keyJoinSQL(keysArray: Array[String]) = {
    var keyJoinSQL = ""
    for (key <- keysArray) {
      keyJoinSQL = keyJoinSQL + " and a. " + key + "=b." + key + ""
    }
    keyJoinSQL
  }

  def buildNotMatchSQL(tableOne: String, tableTwo: String, titleBuffer: Buffer[String], keysArray: Array[String], sortFlag: Boolean): String = {
    var keyMatch = ""
    if (sortFlag) {
      keyMatch = "select a.* from " + tableOne + " a inner join " + tableTwo + " b on 1=1 "
    } else {
      keyMatch = "select a.* from " + tableTwo + " a inner join " + tableOne + " b on 1=1 "
    }
    var valNotMatch = ""
    for (column <- titleBuffer) {
      valNotMatch = valNotMatch + " or a." + column + "<>b." + column + ""
    }
    keyMatch = keyMatch + this.keyJoinSQL(keysArray) + " where 1<>1 " + valNotMatch
    keyMatch
  }

  private def buildSelection(titleList: Array[String], schemasMap: Map[String, String], thresholdsMap: Map[String, String], leftTable: String, rightTable: String): StringBuilder = {
    val sql = new StringBuilder(" SELECT ")
    titleList.foreach(key => {
      val colType = schemasMap.get(key)
      sql.append(this.schemaMatch(key, colType, thresholdsMap))
    })
    sql.deleteCharAt(sql.lastIndexOf(","))
    sql.append(" FROM ").append(leftTable).append(" a INNER JOIN ").append(rightTable).append(" b ")
    sql
  }

  private def buildJoin(keyArrs: Array[String]): String = {
    val sql = new StringBuilder()
    if (keyArrs.length > 0) {
      keyArrs.foreach(key => sql.append(" AND a.").append(key).append(" = b.").append(key))
    }
    val start = sql.indexOf("AND")
    sql.replace(start, (start + 3), "ON").toString()
  }

  private def schemaMatch(key: String, colType: Option[String], thresholdsMap: Map[String, String]): String = {
    colType match {
      case Some("Byte") => {
        if (thresholdsMap.contains(key)) {
          this.absSelection(key, thresholdsMap, "Byte")
        } else {
          this.eqSelection(key)
        }
      }
      case Some("Short") => {
        if (thresholdsMap.contains(key)) {
          this.absSelection(key, thresholdsMap, "Short")
        } else {
          this.eqSelection(key)
        }
      }
      case Some("Int") => {
        if (thresholdsMap.contains(key)) {
          this.absSelection(key, thresholdsMap, "Int")
        } else {
          this.eqSelection(key)
        }
      }
      case Some("Long") => {
        if (thresholdsMap.contains(key)) {
          this.absSelection(key, thresholdsMap, "Long")
        } else {
          this.eqSelection(key)
        }
      }
      case Some("Float") => {
        if (thresholdsMap.contains(key)) {
          this.absSelection(key, thresholdsMap, "Float")
        } else {
          this.eqSelection(key)
        }
      }
      case Some("Double") => {
        if (thresholdsMap.contains(key)) {
          this.absSelection(key, thresholdsMap, "Double")
        } else {
          this.eqSelection(key)
        }
      }
      case Some("BigDecimal") => {
        if (thresholdsMap.contains(key)) {
          this.absSelection(key, thresholdsMap, "BigDecimal")
        } else {
          this.eqSelection(key)
        }
      }
      case _ => {
        this.eqSelection(key)
      }
    }
  }

  private def absSelection(col: String, thresholdsMap: Map[String, String], valueType: String): String = {
    valueType match {
      case "Byte" => {
        s" a.${col} as ${col}_A, b.${col} as ${col}_B, abs(a.${col}-b.${col}) as diff_${col}, case when abs(a.${col}-b.${col})>${thresholdsMap.get(col).get.toByte} then 'Y' else 'N' end as flag_${col}, "
      }
      case "Short" => {
        s" a.${col} as ${col}_A, b.${col} as ${col}_B, abs(a.${col}-b.${col}) as diff_${col}, case when abs(a.${col}-b.${col})>${thresholdsMap.get(col).get.toShort} then 'Y' else 'N' end as flag_${col}, "
      }
      case "Int" => {
        s" a.${col} as ${col}_A, b.${col} as ${col}_B, abs(a.${col}-b.${col}) as diff_${col}, case when abs(a.${col}-b.${col})>${thresholdsMap.get(col).get.toInt} then 'Y' else 'N' end as flag_${col}, "
      }
      case "Long" => {
        s" a.${col} as ${col}_A, b.${col} as ${col}_B, abs(a.${col}-b.${col}) as diff_${col}, case when abs(a.${col}-b.${col})>${thresholdsMap.get(col).get.toLong} then 'Y' else 'N' end as flag_${col}, "
      }
      case "Float" => {
        s" a.${col} as ${col}_A, b.${col} as ${col}_B, abs(a.${col}-b.${col}) as diff_${col}, case when abs(a.${col}-b.${col})>${thresholdsMap.get(col).get.toFloat} then 'Y' else 'N' end as flag_${col}, "
      }
      case "Double" => {
        s" a.${col} as ${col}_A, b.${col} as ${col}_B, abs(a.${col}-b.${col}) as diff_${col}, case when abs(a.${col}-b.${col})>${thresholdsMap.get(col).get.toDouble} then 'Y' else 'N' end as flag_${col}, "
      }
      case "BigDecimal" => {
        s" a.${col} as ${col}_A, b.${col} as ${col}_B, abs(a.${col}-b.${col}) as diff_${col}, case when abs(a.${col}-b.${col})>${thresholdsMap.get(col).get.toDouble} then 'Y' else 'N' end as flag_${col}, "
      }
      case _ => {
        throw new IllegalArgumentException(valueType + " is not support.")
      }
    }
  }

  private def eqSelection(col: String): String = {
    s" a.${col} as ${col}_A, b.${col} as ${col}_B, '' as diff_${col}, case when a.${col}=b.${col} then 'N' else 'Y' end as flag_${col}, "
  }
}
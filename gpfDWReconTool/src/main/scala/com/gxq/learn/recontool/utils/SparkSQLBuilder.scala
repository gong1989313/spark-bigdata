package com.gxq.learn.recontool.utils

object SparkSQLBuilder {
  def buildSQL(titleList: Array[String], keyArrs: Array[String], schemasMap: Map[String, String], thresholdsMap: Map[String, String], leftTable: String, rightTable: String): String = {
    (this.buildSelection(titleList, schemasMap, thresholdsMap, leftTable, rightTable).append(this.buildJoin(keyArrs))).mkString
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
      case Some("Int") => {
        if (thresholdsMap.contains(key)) {
          this.absSelection(key, thresholdsMap, "Int")
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
      case _ => {
        this.eqSelection(key)
      }
    }
  }

  private def absSelection(col: String, thresholdsMap: Map[String, String], valueType: String): String = {
    valueType match {
      case "Int" => {
        s" a.${col} as ${col}_A, b.${col} as ${col}_B, abs(a.${col}-b.${col}) as diff_${col}, case when abs(a.${col}-b.${col})>${thresholdsMap.get(col).get.toInt} then 'Y' else 'N' end as flag_${col}, "
      }
      case "Double" => {
        s" a.${col} as ${col}_A, b.${col} as ${col}_B, abs(a.${col}-b.${col}) as diff_${col}, case when abs(a.${col}-b.${col})>${thresholdsMap.get(col).get.toDouble} then 'Y' else 'N' end as flag_${col}, "
      }
      case _ => {
        throw new IllegalArgumentException(valueType + " is not support.")
      }
    }
  }

  private def eqSelection(col: String): String = {
    s" a.${col} as ${col}_A, b.${col} as ${col}_B, '' as diff_${col}, case when a.${col}=b.${col} then 'Y' else 'N' end as flag_${col}, "
  }
}
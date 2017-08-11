package com.gxq.learn.recontool.utils

import org.apache.poi.ss.usermodel.IndexedColors
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.spark.sql.{ Row => SqlRow }
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import org.apache.poi.ss.usermodel.CellStyle
import scala.util.control.Breaks._

object Csv2ExcelUtil {
  val wb = new XSSFWorkbook

  val introSheet = wb.createSheet("Introduction")
  val keyMValueNMSheet = wb.createSheet("KeyMValueNM")
  val leftSideSheet = wb.createSheet("leftSide")
  val rightSideSheet = wb.createSheet("rightSide")

  val headStyle = wb.createCellStyle()
  headStyle.setFillBackgroundColor(IndexedColors.SKY_BLUE.getIndex)
  headStyle.setFillPattern(CellStyle.ALIGN_CENTER)

  val cellStyle = wb.createCellStyle()
  cellStyle.setFillBackgroundColor(IndexedColors.ORANGE.getIndex)
  cellStyle.setFillPattern(CellStyle.SOLID_FOREGROUND)

  def setHeader(heads: Array[String]): List[String] = {
    heads.toList
  }

  def setCellData(dataList: Array[Array[String]]): List[Array[String]] = {
    dataList.toList
  }

  def writeExcel(lTName: String, rTName: String, ltInRTHead: Array[String], lTInRT: Array[SqlRow],
                 lTNotInRTHead: Array[String], ltNotInRT: Array[SqlRow],
                 rTNotInLTHead: Array[String], rTNotInLT: Array[SqlRow],
                 excelPath: String, rowsCut: Int) = {

  }

  private def createNew1Sheet(collectResult: Array[SqlRow], arrayHead: Array[String], rowsCut: Int) = {
    val head = keyMValueNMSheet.createRow(0)
    for (i <- 0 until this.setHeader(arrayHead).length) {
      val header = head.createCell(i)
      header.setCellStyle(headStyle)
      header.setCellValue(this.setHeader(arrayHead).apply(i))
    }

    val dataArray = this.removeNotDiffRow(collectResult)
    if (dataArray.length >= rowsCut) {
      for (i <- 0 until rowsCut) {
        val dataList = dataArray.apply(i).toList
        val rows = keyMValueNMSheet.createRow(i + 1)
        for (j <- 0 until dataList.length) {
          val rowCell = rows.createCell(j)
          if ("Y".equals(dataList.apply(j).trim) && head.getCell(j).toString().contains("flag")) {
            for (k <- j - 3 until j + 1) {
              val trueRow = rows.createCell(k)
              trueRow.setCellValue(dataList.apply(k))
              trueRow.setCellStyle(cellStyle)
            }
          } else {
            rowCell.setCellValue(dataList.apply(j))
          }
        }
      }
    } else {
      for (i <- 0 until dataArray.length) {
        val dataList = dataArray.apply(i).toList
        val rows = keyMValueNMSheet.createRow(i + 1)
        for (j <- 0 until dataList.length) {
          val rowCell = rows.createCell(j)
          if ("Y".equals(dataList.apply(j).trim) && head.getCell(j).toString().contains("flag")) {
            for (k <- j - 3 until j + 1) {
              val trueRow = rows.createCell(k)
              trueRow.setCellValue(dataList.apply(k))
              trueRow.setCellStyle(cellStyle)
            }
          } else {
            rowCell.setCellValue(dataList.apply(j))
          }
        }
      }
    }

    for (i <- 0 until this.setHeader(arrayHead).length) {
      if (this.setHeader(arrayHead).apply(i).toString().contains("flag")) {
        keyMValueNMSheet.setColumnHidden(i, true)
      }
    }
  }
  
  //private def New2

  private def removeNotDiffRow(collectResult: Array[SqlRow]): List[Array[String]] = {
    val arrayData = collectResult.map(row => eachRow(row))
    val dataArray = this.setCellData(arrayData)
    val removeDataArr = ListBuffer[Array[String]]()
    for (i <- 0 until dataArray.length) {
      val rowData = dataArray.apply(i)
      breakable {
        for (j <- 0 until rowData.length) {
          if ("Y".equals(rowData.apply(j).trim) && wb.getSheet("KeyMValueNM").getRow(0).getCell(j).toString.contains("flag")) {
            removeDataArr += rowData
            break
          }
        }
      }
    }
    removeDataArr.toList
  }

  private def eachRow(row: SqlRow): Array[String] = {
    val dataList = ArrayBuffer[String]()
    for (i <- 0 until row.length) {
      val str = row(i).toString()
      dataList += str
    }
    dataList.toArray
  }

}
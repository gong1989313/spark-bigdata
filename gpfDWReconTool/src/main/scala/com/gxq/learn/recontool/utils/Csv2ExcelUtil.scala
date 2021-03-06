package com.gxq.learn.recontool.utils

import org.apache.poi.ss.usermodel.IndexedColors
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.spark.sql.{ Row => SqlRow }
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import org.apache.poi.ss.usermodel.CellStyle
import scala.util.control.Breaks._
import java.io.FileOutputStream

object Csv2ExcelUtil {
  val wb = new XSSFWorkbook

  val introSheet = wb.createSheet("Introduction")
  val keyMValueNMSheet = wb.createSheet("KeyMValueNM")
  val leftSideSheet = wb.createSheet("leftSide")
  val rightSideSheet = wb.createSheet("rightSide")

  val headStyle = wb.createCellStyle()
  headStyle.setFillBackgroundColor(IndexedColors.SKY_BLUE.getIndex)
  headStyle.setFillPattern(CellStyle.SOLID_FOREGROUND)

  val headIndexStyle = wb.createCellStyle()
  headIndexStyle.setFillBackgroundColor(IndexedColors.MAROON.getIndex)
  headIndexStyle.setFillPattern(CellStyle.SOLID_FOREGROUND)

  val cellStyle = wb.createCellStyle()
  cellStyle.setFillBackgroundColor(IndexedColors.YELLOW.getIndex)
  cellStyle.setFillPattern(CellStyle.SOLID_FOREGROUND)

  private def setHeader(heads: Array[String]): List[String] = {
    heads.toList
  }

  private def setCellData(dataList: Array[Array[String]]): List[Array[String]] = {
    dataList.toList
  }

  def writeExcel(lTName: String, rTName: String, ltInRTHead: Array[String], lTInRT: Array[SqlRow],
                 lTNotInRTHead: Array[String], lTNotInRT: Array[SqlRow],
                 rTNotInLTHead: Array[String], rTNotInLT: Array[SqlRow],
                 excelPath: String, rowsCut: Int, indexRow: Int) = {
    this.createNew1Sheet(lTInRT, ltInRTHead, rowsCut, indexRow)
    this.createNew2Sheet(lTNotInRT, lTNotInRTHead, rowsCut)
    this.createNew3Sheet(rTNotInLT, rTNotInLTHead, rowsCut)
    this.createIntroSheet(lTName, rTName, lTInRT, lTNotInRT, rTNotInLT, ltInRTHead, rowsCut)

    val fileOut = new FileOutputStream(excelPath)
    wb.write(fileOut)
    fileOut.close()
  }

  private def createNew1Sheet(collectResult: Array[SqlRow], arrayHead: Array[String], rowsCut: Int, indexRow: Int) = {
    val headValue = this.setHeader(arrayHead)
    val head = keyMValueNMSheet.createRow(0)
    for (i <- 0 until this.setHeader(arrayHead).length) {
      val header = head.createCell(i)
      if (i < indexRow * 4) {
        header.setCellStyle(headIndexStyle)
        header.setCellValue(headValue.apply(i))
      } else {
        header.setCellStyle(headStyle)
        header.setCellValue(headValue.apply(i))
      }
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
      if (headValue.apply(i).toString().contains("flag")) {
        keyMValueNMSheet.setColumnHidden(i, true)
      }
    }
  }

  private def createNew2Sheet(collectResult: Array[SqlRow], arrayHead: Array[String], rowsCut: Int) = {
    val arrayData = collectResult.map(row => this.eachRow(row))
    val head = leftSideSheet.createRow(0)
    for (i <- 0 until this.setHeader(arrayHead).length) {
      val header = head.createCell(i)
      header.setCellValue(this.setHeader(arrayHead).apply(i))
    }

    val dataArray = this.setCellData(arrayData)
    if (dataArray.length > rowsCut) {
      for (i <- 0 until dataArray.length) {
        val dataList = dataArray.apply(i).toList
        val rows = leftSideSheet.createRow(i + 1)
        for (j <- 0 until dataList.length) {
          val rowCell = rows.createCell(j)
          rowCell.setCellValue(dataList.apply(i))
        }
      }
    } else {
      for (i <- 0 until dataArray.length) {
        val dataList = dataArray.apply(i).toList
        val rows = leftSideSheet.createRow(i + 1)
        for (j <- 0 until dataList.length) {
          val rowCell = rows.createCell(j)
          rowCell.setCellValue(dataList.apply(i))
        }
      }
    }
  }

  private def createNew3Sheet(collectResult: Array[SqlRow], arrayHead: Array[String], rowsCut: Int) = {
    val arrayData = collectResult.map(row => this.eachRow(row))
    val head = rightSideSheet.createRow(0)
    for (i <- 0 until this.setHeader(arrayHead).length) {
      val header = head.createCell(i)
      header.setCellValue(this.setHeader(arrayHead).apply(i))
    }

    val dataArray = this.setCellData(arrayData)
    if (dataArray.length > rowsCut) {
      for (i <- 0 until dataArray.length) {
        val dataList = dataArray.apply(i).toList
        val rows = rightSideSheet.createRow(i + 1)
        for (j <- 0 until dataList.length) {
          val rowCell = rows.createCell(j)
          rowCell.setCellValue(dataList.apply(i))
        }
      }
    } else {
      for (i <- 0 until dataArray.length) {
        val dataList = dataArray.apply(i).toList
        val rows = rightSideSheet.createRow(i + 1)
        for (j <- 0 until dataList.length) {
          val rowCell = rows.createCell(j)
          rowCell.setCellValue(dataList.apply(i))
        }
      }
    }
  }

  private def createIntroSheet(lName: String, rName: String, collectResult: Array[SqlRow], ltNotInRt: Array[SqlRow], rtNotInLt: Array[SqlRow], arrayHead: Array[String], rowsCut: Int) = {
    val arrayData = collectResult.map(row => this.eachRow(row))
    var aBKeyValueNotMatch = 0
    val allKeyMatchValueRow = arrayData.length
    val INANOTINB = ltNotInRt.length
    val INBNOTINA = rtNotInLt.length
    val dataArray = this.setCellData(arrayData)
    for (i <- 0 until dataArray.length) {
      val dataList = dataArray.apply(i).toList
      breakable {
        for (j <- 0 until dataList.length) {
          if ("Y".equals(dataList.apply(i).trim()) && wb.getSheet("KeyMValueNM").getRow(0).getCell(j).toString().contains("flag")) {
            aBKeyValueNotMatch += 1
            break
          }
        }
      }
    }

    val headStyle = wb.createCellStyle()
    headStyle.setFillBackgroundColor(IndexedColors.GREEN.getIndex)
    headStyle.setFillPattern(CellStyle.SOLID_FOREGROUND)

    val headRow1 = introSheet.createRow(0)
    val headValue1 = headRow1.createCell(0)
    headValue1.setCellValue("Table Name")
    headValue1.setCellStyle(headStyle)

    val headValue2 = headRow1.createCell(1)
    headValue2.setCellValue("Table Alias")
    headValue2.setCellStyle(headStyle)

    val data1 = introSheet.createRow(1)
    data1.createCell(0).setCellValue(lName)
    data1.createCell(1).setCellValue("A")

    val data2 = introSheet.createRow(2)
    data2.createCell(0).setCellValue(rName)
    data2.createCell(1).setCellValue("B")

    val headRow2 = introSheet.createRow(4)
    val headValue3 = headRow2.createCell(0)
    headValue3.setCellValue("Recon Type")
    headValue3.setCellStyle(headStyle)

    val headValue4 = headRow2.createCell(1)
    headValue4.setCellValue("Rows")
    headValue4.setCellStyle(headStyle)

    val data4 = introSheet.createRow(5)
    data4.createCell(0).setCellValue("A, B KeyMatchValueMatch")
    data4.createCell(1).setCellValue(allKeyMatchValueRow - aBKeyValueNotMatch)

    val data5 = introSheet.createRow(6)
    data5.createCell(0).setCellValue("A, B KeyMatchValueNotMatch")
    data5.createCell(1).setCellValue(aBKeyValueNotMatch)

    val data6 = introSheet.createRow(7)
    data6.createCell(0).setCellValue("IN A NOT IN B")
    data6.createCell(1).setCellValue(INANOTINB)

    val data7 = introSheet.createRow(8)
    data7.createCell(0).setCellValue("IN B NOT IN A")
    data7.createCell(1).setCellValue(INBNOTINA)

    val headRow3 = introSheet.createRow(10)
    val headValue5 = headRow3.createCell(0)
    headValue5.setCellValue("columnBreak")
    headValue5.setCellStyle(headStyle)

    val headValue6 = headRow3.createCell(1)
    headValue6.setCellValue("Nums")
    headValue6.setCellStyle(headStyle)

    val data11 = introSheet.createRow(11)

    val cellMap = this.getCellBreakNum(collectResult, arrayHead, rowsCut)
    val keysList = cellMap.keys.toList
    for (i <- 0 until keysList.length) {
      val dataRow = introSheet.createRow(i + 11)
      dataRow.createCell(0).setCellValue(keysList.apply(i))
      dataRow.createCell(1).setCellValue(cellMap.get(keysList.apply(i)).get)
    }

    introSheet.autoSizeColumn(0)
    introSheet.autoSizeColumn(1)
  }

  private def getCellBreakNum(collectResult: Array[SqlRow], arrayHead: Array[String], rowsCut: Int): Map[String, Int] = {
    val headValue = this.setHeader(arrayHead)
    val dataArray = this.removeNotDiffRow(collectResult)

    var cellMap: Map[String, Int] = Map()
    for (i <- 0 until headValue.length) {
      if (headValue.apply(i).contains("flag")) {
        cellMap += (headValue.apply(i).substring(5) -> 0)
      }
    }

    for (i <- 0 until dataArray.length) {
      val rowData = dataArray.apply(i).toList
      for (j <- 0 until rowData.length) {
        if (headValue.apply(j).contains("flag") && "Y".equals(rowData.apply(j))) {
          cellMap += (headValue.apply(i).substring(5) -> (cellMap.get(headValue.apply(j).substring(5)).get + 1))
        }
      }
    }
    cellMap
  }

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
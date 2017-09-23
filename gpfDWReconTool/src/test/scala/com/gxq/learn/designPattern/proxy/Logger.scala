package com.gxq.learn.designPattern.proxy

import scala.collection.mutable.ArrayBuffer
import com.gxq.learn.designPattern.proxy.dao.AccessDAO

/**
 * 日志记录类
 */
object Logger {
  /**
   * 记录日志
   * @param id 登录id
   */
  def log(id: String): Unit = {
    val params = new ArrayBuffer[Any]()
    params += id
    val row = AccessDAO.insertLog(params)
    if (row > 0) {
      println(s"记录$id 到数据库")

    } else {
      println("出现异常")
    }

  }
}
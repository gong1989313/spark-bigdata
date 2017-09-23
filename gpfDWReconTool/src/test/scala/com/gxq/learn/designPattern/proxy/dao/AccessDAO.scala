package com.gxq.learn.designPattern.proxy.dao

import scala.collection.mutable.ArrayBuffer
import com.gxq.learn.designPattern.proxy.dao.utils.MySQLDBConn

/** * 代理帐号操作数据库对象 * Created by ctao on 2015/8/29. */
object AccessDAO {
  /**     
   ** 查询sql
   **/
  private val sqlSelect = "select name,pass from user where name = ?"
  
  /**     
   ** 查询     
   ** @param params 参数列表     
   ** @return ArrayBuffer     
   **/
  def checkUser(params: ArrayBuffer[Any]) = MySQLDBConn.Result(sqlSelect, params)
  
  /**     
   ** 插入日志列表sql     
   **/
  private val sqlInsert = "insert into log(userid) values(?)"
  
  /**     
   ** 插入操作   
   ** @param params 参数    
   ** @return 受影响行数    
   **/
  def insertLog(params: ArrayBuffer[Any]) = MySQLDBConn.updateRow(sqlInsert, params)
}
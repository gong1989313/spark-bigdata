package com.gxq.learn.designPattern

import scala.collection.mutable.ArrayBuffer
import com.gxq.learn.designPattern.proxy.dao.AccessDAO

/**
 * 身份验证业务单例对象
 */
object AccessValidator {
  /**
     * 验证方法
     * @param id 查询的id
     * @param pass 用户名
     * @return 用户是否合法
     */
    def validate(id: String, pass: String): Boolean = {
        println(s"数据库验证$id 是否为合法用户")
        val params = new ArrayBuffer[Any]()
        params += id
        val result = AccessDAO.checkUser(params)

        /**
         * 判断result.head获取第一个元素的name和result.head获取第一个元素的pass和传入的id和pass是否一致
         * 一致返回true，否则返回false
         */
        if (result.head.getOrElse("name", "null") == id && result.head.getOrElse("pass", "null").toString == pass) {
            println("登录成功")
            true
        } else {
            println("登录失败")
            false
        }
    }
}
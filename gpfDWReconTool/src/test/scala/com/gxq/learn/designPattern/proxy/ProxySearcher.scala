package com.gxq.learn.designPattern

import com.gxq.learn.designPattern.proxy.Searcher
import com.gxq.learn.designPattern.proxy.Logger

/**
 * 代理主题类：代理查询类
 */
object ProxySearcher extends Searcher {
    /**
     * 真实查询对象
     */
    private val realSearcher = RealSearcher
    /**
     * 身份验证对象
     */
    private val accessValidator = AccessValidator
    /**
     * 日志对象
     */
    private val logger = Logger

    /**
     * 复写查询
     * @param id 用户id
     * @param pass 密码
     * @param keyword 关键字
     * @return 查询结果
     */
    override def doSearch(id: String, pass: String, keyword: String): String = {
        /**
         * 判断是否登录成功，如果登录成功则记录到数据库中，并执行真实查询类的查询方法
         */
        if (validate(id, pass)) {
            log(id)
            realSearcher.doSearch(id, pass, keyword)
        } else {
            null
        }
    }

    /**
     * 日志方法，使用日志对象的日志方法
     * @param id 用户id
     */
    def log(id: String): Unit = logger.log(id)

    /**
     * 身份验证类
     * @param id 用户id
     * @param pass 密码
     * @return 是否验证成功
     */
    def validate(id: String, pass: String): Boolean = accessValidator.validate(id, pass)
}
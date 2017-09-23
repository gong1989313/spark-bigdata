package com.gxq.learn.designPattern

import com.gxq.learn.designPattern.proxy.Searcher

/**
 * 真实主题类：具体查询类
 */
object RealSearcher extends Searcher {
    /**
     * 复写查询方法
     * @param id 用户id
     * @param pass pass
     * @param keyword 关键字
     * @return 查询内容
     */
    override def doSearch(id: String, pass: String, keyword: String): String = {
        println(s"用户：$id 使用关键字$keyword 查询商务信息！")
        "具体内容"
    }
}

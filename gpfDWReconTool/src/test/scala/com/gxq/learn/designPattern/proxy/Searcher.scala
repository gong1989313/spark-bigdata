package com.gxq.learn.designPattern.proxy

/**
 * 抽象主题类：查询特质
 */
abstract class Searcher {
  def doSearch(id: String, pass: String, keyword: String): String
}
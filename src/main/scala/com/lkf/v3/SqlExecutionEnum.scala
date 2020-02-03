package com.lkf.v3

/**
  * sql 执行类型枚举
  *
  * @author kaifeng
  **/
object SqlExecutionEnum extends Enumeration {
  //报表计算日志
  val SQL_REPORT = Value(1, "loongboss_cmp_server_log")
  //SQL执行计划及统计信息
  val SQL_SUMMARY = Value(2, "loongboss_cmp_summary")
  //SQL执行计划及统计信息
  val PREDICTION_DATA = Value(3, "loongboss_prediction_data")
}

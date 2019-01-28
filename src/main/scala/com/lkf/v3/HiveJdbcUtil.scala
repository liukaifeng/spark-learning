package com.lkf.v3

import org.apache.livy.client.ext.model.SparkSqlCondition
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * todo 一句话描述该类的用途
  *
  * @author 刘凯峰
  */
object HiveJdbcUtil {
  //hive URL  地址
  private val hiveUrl = "hiveUrl"
  //hive 用户名
  private val hiveUser = "hiveUser"
  //hive 密码
  private val hivePassword = "hivePassword"


  //  def loadData2DataFrame(sparkSession: SparkSession, sql: String): DataFrame = {
  //    val sparkSqlCondition: SparkSqlCondition = new SparkSqlCondition()
  //    sparkSqlCondition.setKeyspace("default")
  //    val hiveJdbcUrl = "jdbc:hive2://192.168.12.204:21050/default;auth=noSasl"
  //    val table = String.format("(%s) as T", sql)
  //    //加载数据到dataframe
  //    var df: DataFrame = sparkSession.read
  //      .format("jdbc")
  //      .options(Map("driver" -> "org.apache.hive.jdbc.HiveDriver",
  //        "url" -> hiveJdbcUrl,
  //        "dbtable" -> table,
  //        "user" -> "",
  //        "password" -> "")
  //      )
  //      .load()
  //    df
  //  }

  /**
    * 通过hive jdbc 加载数据
    *
    * @param sparkSession      spark 上下文
    * @param sparkSqlCondition 计算条件及hive配置信息
    **/
  def loadData2DataFrame(sparkSession: SparkSession, sparkSqlCondition: SparkSqlCondition, sql: String): DataFrame = {
    import org.apache.spark.sql.jdbc.JdbcDialects
    JdbcDialects.registerDialect(HiveDialect)
    //数据库名称
    var dataBaseName = sparkSqlCondition.getKeyspace
    //hive jdbc配置项
    val hiveJdbcMap = sparkSqlCondition.getHiveJdbcConfig
    // 替换掉数据库前缀
    val preSql = sql.replace(dataBaseName.concat("."), "")
    if (dataBaseName.contains("impala")) {
      dataBaseName = dataBaseName.replace("impala::", "")
    }
    //hive jdbc URL地址
    val hiveJdbcUrl = String.format(hiveJdbcMap.get("hiveUrl"), dataBaseName)
    //hive jdbc table
    val table = String.format("(%s) as T", preSql)

    //加载数据到dataframe
    var df: DataFrame = sparkSession.read
      .format("jdbc")
      .options(Map("driver" -> "org.apache.hive.jdbc.HiveDriver",
        "url" -> hiveJdbcUrl,
        "dbtable" -> table,
        "user" -> hiveJdbcMap.getOrDefault(hiveUser, ""),
        "password" -> hiveJdbcMap.getOrDefault(hivePassword, ""))
      )
      .load()
    df
  }

}

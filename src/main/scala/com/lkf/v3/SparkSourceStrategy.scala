package com.lkf.v3

import org.apache.kudu.spark.kudu._
import org.apache.livy.client.ext.model.SparkSqlCondition
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * session 初始化策略抽象接口
  */
trait ISessionStrategy {
  /**
    * 会话配置初始化
    *
    * @param sparkSqlCondition 配置项
    **/
  def sessionInit(sparkSqlCondition: SparkSqlCondition): SparkSession
}

/**
  * spark session 初始化
  */

class SparkHdfsStrategy(sparkConf: SparkConf) extends ISessionStrategy {

  /**
    * 会话配置初始化
    *
    * @param sparkSqlCondition 配置项
    **/
  override def sessionInit(sparkSqlCondition: SparkSqlCondition): SparkSession = {
    import org.apache.spark.sql.jdbc.JdbcDialects
    JdbcDialects.registerDialect(HiveDialect)
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    sparkSession
  }
}

/**
  * spark kudu session 初始化
  */
class SparkKuduStrategy(sparkConf: SparkConf) extends ISessionStrategy {

  override def sessionInit(sparkSqlCondition: SparkSqlCondition): SparkSession = {
    //构造spark session
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    sparkSession.sparkContext.setLogLevel("info")
    val kuduTable = sparkSqlCondition.getKeyspace.concat(".").concat(sparkSqlCondition.getTable)
    //加载数据表
    var df: DataFrame = sparkSession
      .read
      .options(Map("kudu.master" -> "hadoop207", "kudu.table" -> kuduTable))
      .kudu
      .filter(sparkSqlCondition.getCassandraFilter)

    val mainSql = sparkSqlCondition.getSelectSql

    //临时表命名
    val tempView = "temp_kudu_table".concat(System.currentTimeMillis().toString)
    //创建临时表
    df.createOrReplaceTempView(tempView)
    sparkSqlCondition.setSelectSql(mainSql.replace(kuduTable, tempView))
    sparkSession
  }
}


/**
  * spark hive jdbc 会话配置
  **/
class SparkHiveJdbcStrategy(sparkConf: SparkConf) extends ISessionStrategy {

  /**
    * 会话配置初始化
    *
    * @param sparkSqlCondition 配置项
    **/
  override def sessionInit(sparkSqlCondition: SparkSqlCondition): SparkSession = {
    //    import org.apache.spark.sql.jdbc.JdbcDialects
    //    JdbcDialects.registerDialect(HiveJDBC)

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    //    sparkSession.sparkContext.setLogLevel("info")
    //    val kuduTable = sparkSqlCondition.getKeyspace.concat(".").concat(sparkSqlCondition.getTable)
    //    var dataBaseName = sparkSqlCondition.getKeyspace
    //    val hiveJdbcMap = sparkSqlCondition.getHiveJdbcConfig
    //    if (sparkSqlCondition.getKeyspace.contains("impala")) {
    //      dataBaseName = sparkSqlCondition.getKeyspace.replace("impala::", "")
    //    }
    //
    //    val hiveJdbcUrl = String.format(hiveJdbcMap.get("hiveUrl"), dataBaseName)
    //    val table = String.format("(select * from %s where %s) as T", sparkSqlCondition.getTable, sparkSqlCondition.getCassandraFilter)
    //    var df: DataFrame = sparkSession.read
    //      .format("jdbc")
    //      .options(Map("driver" -> "org.apache.hive.jdbc.HiveDriver",
    //        "url" -> hiveJdbcUrl,
    //        "dbtable" -> table,
    //        "user" -> hiveJdbcMap.getOrDefault("hiveUser", ""),
    //        "password" -> hiveJdbcMap.getOrDefault("hivePassword", ""),
    //        "numPartitions" -> "20"))
    //      .load()
    //    //      .filter(sparkSqlCondition.getCassandraFilter)
    //
    //    //临时表命名
    //    val tempView = "temp_hive_table_".concat(System.currentTimeMillis().toString)
    //    //创建临时表
    //    df.createOrReplaceTempView(tempView)
    //    sparkSqlCondition.setSelectSql(sparkSqlCondition.getSelectSql.replace(kuduTable, tempView))
    //    sparkSqlCondition.setSelectQoqSql(sparkSqlCondition.getSelectQoqSql.replace(kuduTable, tempView))
    sparkSession
  }
}


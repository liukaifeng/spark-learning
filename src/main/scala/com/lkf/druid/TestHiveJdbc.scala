package com.lkf.druid

import com.lkf.v3.HiveDialect
import org.apache.spark.SparkConf


import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

/**
  * todo 一句话描述该类的用途
  *
  * @author 刘凯峰
  * @date 2019-07-09 21-51
  */
object TestHiveJdbc {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.jdbc.JdbcDialects
    JdbcDialects.registerDialect(HiveDialect)
    val conf = new SparkConf().setAppName("JdbcRDDTest").setMaster("local[4]")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    sparkSession.sparkContext.setLogLevel("info")

    val table = "(select * from %s where %s) as T"
    var df: DataFrame = sparkSession.read
      .format("jdbc")
      .options(Map("driver" -> "org.apache.hive.jdbc.HiveDriver",
        "url" -> "jdbc:hive2://192.168.12.204:21050/e000;auth=noSasl",
        "dbtable" -> table,
        "user" -> "",
        "password" ->"",
        "numPartitions" -> "20"))
      .load()
    //      .filter(sparkSqlCondition.getCassandraFilter)


    sparkSession
  }

}

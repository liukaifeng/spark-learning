package com.lkf.impala

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * todo 一句话描述该类的用途
  *
  * @author 刘凯峰
  * @date 2019-07-30 14-50
  */
object SparkImpalaDemo {
  def main(args: Array[String]): Unit = {
    var sparkConf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("spark_sql_test")
    val table = "(SELECT\n  storeid,\n  openid,\n  CAST(SUM(realprice) AS STRING) AS consume_bill_amount\nFROM\n  wx.kudu_wuxiang_src_pho_order\nWHERE storeid = '19912'\n  AND to_date (CREATETIME) >= DATE_SUB(NOW(), 90)\n  AND isdownsuccess = 5\nGROUP BY storeid,\n  openid\nHAVING SUM(realprice) >= 100) as T"
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val connectionProperties = new Properties()
    connectionProperties.put("user", "")
    connectionProperties.put("password", "")
    connectionProperties.put("driver", "org.apache.hive.jdbc.HiveDriver")
    val df = spark.read.jdbc("jdbc:hive2://192.168.12.204:21050/default;auth=noSasl", table, connectionProperties)
    df.rdd.map(row=>println(row.get(0)))
//    df.rdd.saveAsTextFile("hdfs://ns1/kaifeng/data/123.txt")
  }
}

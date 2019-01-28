package com.lkf.v3

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}


object SparkImpala {
  def main(args: Array[String]): Unit = {
    var sparkConf = new SparkConf()
      .set("spark.default.parallelism", "1")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.executor.instances", "1")
      .set("spark.driver.cores", "1")
      .set("spark.executor.cores", "1")
      .set("spark.extraListeners", "com.lkf.v3.MySparkAppListener")
      .setMaster("local[2]")
      .setAppName("spark_sql_default")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    //    val hiveJdbcUrl = "jdbc:hive2://192.168.12.204:21050/default;auth=noSasl"
    val hiveJdbcUrl = "jdbc:impala://192.168.12.204:21050/default;auth=noSasl"

    var df: DataFrame = sparkSession.read
      .format("jdbc")
      .options(Map("driver" -> "org.apache.hive.jdbc.HiveDriver",
        "dbtable" -> "(show databases) as T",
        "url" -> hiveJdbcUrl,
        "user" -> "",
        "password" -> ""))
      .load()
  }

}

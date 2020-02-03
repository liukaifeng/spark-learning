//package com.lkf.redis
//
//import org.apache.spark.sql.SparkSession
//import com.redislabs.provider.redis._
//
///**
//  * @author 刘凯峰
//  * @date 2019-12-24 09:14
//  * @description TODO
//  * @version V1.0
//  */
//object SparkRedisDemo {
//  def main(args: Array[String]): Unit = {
//    val spark: SparkSession = SparkSession
//      .builder()
//      .appName("redis-df")
//      .master("local[*]")
//      .config("spark.redis.host", "192.168.5.103")
//      .config("spark.redis.port", "6379")
//      //      .config("spark.redis.auth", "root")
//      .getOrCreate()
//    val sc = spark.sparkContext
//
//    spark.read.parquet("hdfs://hadoop-slave2:6020/user/hive/warehouse/wx.db/hdfs_wuxiang_customer_care_part_20191223").createOrReplaceTempView("tb_customer")
//    val df= spark.sql("SELECT groupCode,openId FROM tb_customer LIMIT 50000").toJSON
//
//    df.repartition(20).write
//      .format("org.apache.spark.sql.redis")
//      .option("table", "group")
//      .save()
//  }
//}
//

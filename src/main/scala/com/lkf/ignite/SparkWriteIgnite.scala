package com.lkf.ignite

import org.apache.ignite.spark.IgniteDataFrameSettings
import org.apache.kudu.spark.kudu._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * todo 一句话描述该类的用途
  *
  * @author 刘凯峰
  * @date 2019-07-26 16-10
  */
object SparkWriteIgnite {
  private val CONFIG = "D:\\workspace\\IDEA2017\\spark-learning\\src\\main\\resources\\config\\default-config.xml"
  val kudeMaster = "hadoop-slave2:7051,hadoop-slave5:7051,hadoop-slave4:7051"
  val kuduTable = "impala::wx.wx_customer_consume_v1"

  def main(args: Array[String]): Unit = {
    //    sparkReadKuduWriteIgnite(kudeMaster, kuduTable)
    readFromHdfs()
  }

  def sparkReadKuduWriteIgnite(kudeMaster: String, kuduTable: String): Unit = {
    val spark = SparkSession.builder.appName("Spark Ignite data sources write example").master("local").config("spark.executor.instances", "2").getOrCreate
    val kuduDataFrame: DataFrame = spark.read
      .options(Map("kudu.master" -> "hadoop-slave2:7051,hadoop-slave5:7051,hadoop-slave4:7051", "kudu.table" -> "impala::wx.wx_customer_consume_v1")).kudu

    kuduDataFrame.createTempView("temp_kudu")
    kuduDataFrame.sqlContext.sql("SELECT storecode, openid, lasttime, consumeamount, totalnum, totalamount, consumeintervals, mostdishs, evaluationstarts, evaluationcontent,now() FROM temp_kudu")
    //.option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "storecode,openid")
    kuduDataFrame.write
      .format(IgniteDataFrameSettings.FORMAT_IGNITE)
      .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE, CONFIG)
      .option(IgniteDataFrameSettings.OPTION_STREAMER_ALLOW_OVERWRITE, true)
      .option(IgniteDataFrameSettings.OPTION_TABLE, "wx_customer_consume_v2")
      .mode(SaveMode.Append).save()
    spark.close()
  }

  def readFromHdfs(): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .config(new SparkConf().setAppName("SparkMysql").setMaster("local[5]"))
      .getOrCreate
    val jsonDataFrame = spark.read.json("D:\\partrion\\wx2\\20190731\\*")
    jsonDataFrame.createTempView("temp_kudu")
    jsonDataFrame.sqlContext.sql("SELECT storecode, openid, lasttime, consumeamount, totalnum, totalamount, consumeintervals, mostdishs, evaluationstarts, evaluationcontent,now() FROM temp_kudu")

    jsonDataFrame.show()
    jsonDataFrame.write
      .format(IgniteDataFrameSettings.FORMAT_IGNITE)
      .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE, CONFIG)
      .option(IgniteDataFrameSettings.OPTION_STREAMER_ALLOW_OVERWRITE, true)
      .option(IgniteDataFrameSettings.OPTION_TABLE, "wx_customer_consume_v2")
      .mode(SaveMode.Append).save()
    spark.close()
  }
}

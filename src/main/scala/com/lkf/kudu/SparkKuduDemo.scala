package com.lkf.kudu

import org.apache.kudu.spark.kudu._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkKuduDemo {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = newSparkSession()
    spark.sparkContext.setLogLevel("INFO")
    val s = System.currentTimeMillis()
    val kuduDataFrame: DataFrame = spark.read.options(Map("kudu.master" -> "hadoop-slave2:7051,hadoop-slave5:7051,hadoop-slave4:7051", "kudu.table" -> "(select * from impala::e000.dw_trade_bill_fact_p_group limit 5) as T")).kudu
    //    kuduDataFrame.filter("group_code=18291").show()
    //    kuduDataFrame.show()  where group_code='9759'
    kuduDataFrame.createTempView("temp_kudu")
    kuduDataFrame.sqlContext.sql("select  store_name as `ljc_group_x_store_name_0` , sum(people_qty) as `ljc_sum_x_people_qty_0`  from temp_kudu where group_code='9759'  group by `ljc_group_x_store_name_0`")
    kuduDataFrame.show()
    println(System.currentTimeMillis() - s)
    //    kuduDataFrame.show()
    //    System.err.println("group_code  总数据条数：" + kuduDataFrame.count())
    //    val kuduContext = new KuduContext("kudu.master:7051", spark.sparkContext)
  }

  private[this] def newSparkSession(): SparkSession = {
    var sparkConf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("spark_sql_test")

    //      .set("spark.default.parallelism", "1")
    //      .set("spark.sql.shuffle.partitions", "1")
    //      .set("spark.executor.instances", "1")
    //      .set("spark.driver.cores", "1")
    //      .set("spark.executor.cores", "1")
    val sparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
    sparkSession
  }
}

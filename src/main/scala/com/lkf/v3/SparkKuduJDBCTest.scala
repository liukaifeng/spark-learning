package com.lkf.v3

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkKuduJDBCTest {

  var HIVE_METASTORE_URIS = "hvie.metastore.uris";
  var HIVE_METASTORE_LOCAL= "hive.metastore.local";
  var SPARK_EXECUTOR_INSTANCE = "spark.executor.instances";
  var SPARK_EXECUTOR_MEMORY = "spark.executor.memory";
  var SPARK_EXECUTOR_CORES = "spark.executor.cores";
  var SPARK_DRIVER_MEMORY = "spark.driver.memory";
  var SPARK_DRIVER_CORES = "spark.driver.cores";
  var SPARK_SQL_SHUFFLE_PARTITIONS = "spark.sql.shuffle.partitions";
  var SPARK_DEFAULT_PARALLELISM = "spark.default.parallelism";
  var SPARK_MASTER = "local[12]";
  //  var SPARK_MASTER = "yarn-client";
  var SPARK_APPNAME = "wcltest";

  def main(args: Array[String]): Unit = {
    new ThreadExample().start();
  }

  class ThreadExample extends Thread{
    System.setProperty("SPARK_SCALA_VERSION","2.11")

    override def run(){
      println("准备开始")
      //      Thread.sleep(10000)
      println("开始执行")
      import org.apache.spark.sql.jdbc.JdbcDialects
      JdbcDialects.registerDialect(HiveDialect)
      var session = SparkSession.builder()

        .config(HIVE_METASTORE_URIS,"thrift://192.168.4.22:9083")
        .config(HIVE_METASTORE_LOCAL,false)
        .config(SPARK_EXECUTOR_MEMORY,"2g")
        .config(SPARK_EXECUTOR_CORES,4)
        .config(SPARK_DRIVER_MEMORY,"12g")
        .config("spark.executor.heartbeatInterval",10000)
        .config("spark.network.timeout",100000)
        //      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
        .config("spark.broadcast.checksum",false)
        //      .config("spark.cleaner.referenceTracking.cleanCheckpoints",true)
        .config("spark.memory.storageFraction",0.1)
        .config("spark.sql.streaming.unsupportedOperationCheck",false)
        .config("spark.shuffle.compress",false)
        .config(SPARK_DRIVER_CORES,1)
        .config(SPARK_SQL_SHUFFLE_PARTITIONS,12)
        .config(SPARK_DEFAULT_PARALLELISM,12)
        .config(SPARK_EXECUTOR_INSTANCE,2)
        //        .config("spark.sql.codegen",false)
        .config("spark.sql.sources.partitionColumnTypeInference.enabled",false)
        .config("spark.debug.maxToStringFields",1000)
        .config("spark.sql.codegen.wholeStage",false)
        .master(SPARK_MASTER)
        .config("spark.submit.deployMode","client")
        .config("spark.some.config.option", "config-value")
        .config("spark.sql.variable.substitute",false)
        .config("spark.scheduler.mode","FAIR")
        //.config("spark.shuffle.manager","sort")
        .appName(SPARK_APPNAME)
        .config("spark.logLineage",true)
        .enableHiveSupport()
        .getOrCreate()
      //加载数据表
      var df: DataFrame = session
        .read.format("jdbc")
        .options(Map("driver" -> "org.apache.hive.jdbc.HiveDriver",
          "url" -> "jdbc:hive2://192.168.12.204:21050/e000;auth=noSasl",
          "dbtable" -> "(select group_code, cost_price  cost_price from dw_trade_bill_detail_fact_p_group  ) as test",
          "user" -> "",
        "password"->"")
        ).load()
//      df.createOrReplaceTempView("test")
//      session.sparkContext.parallelize(
//        1 to 4,5
//      )
      //      .filter("1=1 ").
      //    //临时表命名
      var start = System.currentTimeMillis()

      //8391 9759 14810 18291
             df.select("group_code","cost_price").show()

      //    val tempView = "temp_kudu_table".concat(System.currentTimeMillis().toString)
      //    //创建临时表
      //    df.
      //    df.show(100)
      //    println( df.count())

      //    df.groupBy("group_code").sum("pay_money").show()
      var end = System.currentTimeMillis()
      println(end-start)
      while(true){
        Thread.sleep(100000)
      }
    }

  }
}

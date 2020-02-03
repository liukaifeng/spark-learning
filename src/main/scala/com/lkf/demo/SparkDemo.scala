package com.lkf.demo

import org.apache.spark.sql.Row
import org.json4s._

/**
  * todo 一句话描述该类的用途
  *
  * @author 刘凯峰
  */
object SparkDemo {

  case class Prediction(settle_biz_date: String, amount: Double, weather: String, temperature: Double)

  def main(args: Array[String]): Unit = {
    //    val sparkSession = SparkSession.builder()
    //      .master("local")
    //      .appName("test")
    //      .config("spark.default.parallelism", "1")
    //      .config("spark.sql.shuffle.partitions", "1")
    //      .config("spark.executor.instances", "1")
    //      .getOrCreate()


    //    //1)toDF
    //    import sparkSession.implicits._
    //    val df = Seq(
    //      ("2019-07-20", java.math.BigDecimal.valueOf(1260.35), "晴天", 30),
    //      ("2019-07-22", java.math.BigDecimal.valueOf(100.35), "阴天", 36),
    //      ("2019-07-23", java.math.BigDecimal.valueOf(658.35), "小雨", 12),
    //      ("2019-07-24", java.math.BigDecimal.valueOf(9848.35), "中雨", 35)
    //    ).toDF("settle_biz_date", "amount", "weather", "temperature")
    //
    //    //2）
    //    val people = sparkSession.sparkContext.textFile("G:\\sougou_user_search_log.txt").map(_.split("\\t"))
    //    df.show()

    implicit def formats = DefaultFormats
    import org.apache.spark.sql.SparkSession
    // configure spark// configure spark
    val spark = SparkSession.builder.appName("Spark Example - Read JSON to RDD").master("local[2]").getOrCreate
    //    // read json to RDD
    val jsonPath = "G:\\workspace\\python3\\predict.json"
    val items = spark.sparkContext.textFile("/kaifeng/sougou_user_search_log.txt").map(_.split("\\s")).map(r => Row.fromSeq(r)).toJavaRDD
    import org.apache.spark.sql.types._
    val schema = StructType(List(
      StructField("access_time", StringType, nullable = false),
      StructField("user_id", StringType, nullable = true),
      StructField("search_world", StringType, nullable = true),
      StructField("url_ranking", StringType, nullable = true),
      StructField("user_click_num", StringType, nullable = true),
      StructField("user_click_url", StringType, nullable = true),
      StructField("search_world", StringType, nullable = true)
    ))
    val txtDf = spark.createDataFrame(items, schema)
    txtDf.createTempView("sougou_db")
    spark.sql("select * from sougou_db limit 10").show()

    //    import spark.implicits._
    //    var json = "[{\"settle_biz_date\":\"2019-07-21\",\"recv_money\":\"360.59\",\"nightweather\":\"晴天\",\"nighttemperature\":\"30\"},{\"settle_biz_date\":\"2019-07-22\",\"recv_money\":\"158.36\",\"nightweather\":\"阴天\",\"nighttemperature\":\"36\"},{\"settle_biz_date\":\"2019-07-23\",\"recv_money\":\"169\",\"nightweather\":\"小雨\",\"nighttemperature\":\"35\"}]"
    //    var ds = spark.createDataset(Seq(json))
    //    val df = spark.read.json(ds)
    //    // 10-获取数据结构
    //    val jsonString = df.schema.json
    //    val jSchema = parse(jsonString)
    //    // 11-获取数据
    //    val rows = df.take(100).map(_.toSeq)
    //    val jRows = ExtractionUtil.decompose(rows)
    //    println(jSchema)
    //    println(jRows)
  }
}

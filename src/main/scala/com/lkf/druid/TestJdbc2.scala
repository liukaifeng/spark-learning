package com.lkf.druid

import java.sql.{Connection, ResultSet}

import com.lkf.v3.HiveJdbcPoolUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.json4s._
import org.json4s.jackson.Serialization

import scala.collection.JavaConverters._

object TestJdbc2 {
  def main(args: Array[String]): Unit = {
    val conn: Connection = HiveJdbcPoolUtil.getConnection.get
    val sql = "SELECT\n  di3lie AS `ljc_group_x_di3lie1548050049000_0`,\n  SUM(CAST(di9lie AS DOUBLE)) AS `ljc_sum_x_di9lie1548050987000_0`,\n  AVG(CAST(di10lie AS DOUBLE)) AS `ljc_avg_x_di10lie1548050989000_0`,\n  AVG(CAST(di11lie AS DOUBLE)) AS `ljc_avg_x_di11lie1548051313000_0`\nFROM\n  e000112.shujuquanbiao2019maying_sheet1_000112\nGROUP BY `ljc_group_x_di3lie1548050049000_0`\nORDER BY `ljc_avg_x_di10lie1548050989000_0` DESC NULLS LAST\nLIMIT 1500"
    val ps = conn.prepareStatement(sql)
    val rs: ResultSet = ps.executeQuery
    val col = rs.getMetaData.getColumnCount
    //获取元数据
    val dialect = JdbcDialects.get(HiveJdbcPoolUtil.druidProps.getProperty("url"))
    val schema: StructType = JdbcUtils.getSchema(rs, dialect)
    val rows: List[Row] = JdbcUtils.resultSetToRows(rs, schema).toList

    implicit val formats = Serialization.formats(ShortTypeHints(List()))
    val data = Extraction.decompose(rows)
    println(schema)
    println(data)

    //需要二次计算的情况将数据集转换为dataframe
    val conf = new SparkConf().setAppName("TestJDBC").setMaster("local[2]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val df: DataFrame = spark.createDataFrame( rows.asJava, schema)
    df.show()

  }
}

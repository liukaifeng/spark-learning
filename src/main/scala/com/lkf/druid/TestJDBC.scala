package com.lkf.druid

import java.sql.{Connection, ResultSet}

import com.lkf.v3.{HiveDialect, HiveJdbcPoolUtil}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.types.StructType

object TestJDBC {
  var resultSet: ResultSet = null

  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.jdbc.JdbcDialects
    JdbcDialects.registerDialect(HiveDialect)
    val conf = new SparkConf().setAppName("TestJDBC").setMaster("local[2]")

    val spark:SparkSession = SparkSession.builder().config(conf).getOrCreate()

    try {
      for (i <- 1 to 3) {
        val connection = () => {
          //        Class.forName("org.apache.hive.jdbc.HiveDriver").newInstance()
          HiveJdbcPoolUtil.getConnection.get
          //        DriverManager.getConnection("jdbc:hive2://192.168.12.204:21050/e000;auth=noSasl", "", "")
        }
        val sql = "(SELECT\n  store_name AS `ljc_group_x_store_name1545293502000_0`,\n  CAST(group_name AS STRING) AS `ljc_compare_x_group_name1548756742000_0`,\n  SUM(CAST(people_qty AS DOUBLE)) AS `ljc_sum_x_people_qty1545293568000_0`\nFROM\n  e000.dw_trade_bill_fact_p_group_n2\nWHERE group_code = 9759\nGROUP BY `ljc_group_x_store_name1545293502000_0`,\n  `ljc_compare_x_group_name1548756742000_0`) AS T"
        val jrdd = new JDBCRDD(spark.sparkContext, connection, sql, extractValues)
        val caseInsensitiveOptions = CaseInsensitiveMap(
          Map("driver" -> "org.apache.hive.jdbc.HiveDriver",
            "url" -> "jdbc:hive2://192.168.12.204:21050/e000;auth=noSasl",
            "dbtable" -> sql,
            "user" -> "",
            "password" -> ""))
        val jdbcOptions = new JDBCOptions(caseInsensitiveOptions)
        val st = resolveTable(jdbcOptions, HiveJdbcPoolUtil.getConnection.get)
        println(st)

      }

      spark.stop()
    }
    catch {
      case e: Exception => println(e.printStackTrace())
    }

  }

  def extractValues(r: ResultSet): Unit = {
    resultSet = r
  }

  def resolveTable(options: JDBCOptions, conn: Connection): StructType = {
    val url = options.url
    val table = options.table
    val dialect = HiveDialect // JdbcDialects.get(url)
    //    val conn: Connection = JdbcUtils.createConnectionFactory(options)()
    try {
      val statement = conn.prepareStatement(dialect.getSchemaQuery(table))
      try {
        val rs = statement.executeQuery()
        try {
          JdbcUtils.getSchema(rs, dialect, alwaysNullable = true)
        } finally {
          rs.close()
        }
      } finally {
        statement.close()
      }
    } finally {
      conn.close()
    }
  }
}


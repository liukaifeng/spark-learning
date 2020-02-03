package com.lkf.demo

import java.sql.{Connection, ResultSet}

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.StructType
import org.json4s.jackson.Serialization


object ImpalaJdbcDemo {
  def main(args: Array[String]): Unit = {
    val conn: Connection = getConnection
    val sql = "SELECT\n\ta.storeid,\n\ta.openid\nFROM\n\twx.kudu_wuxiang_src_pho_order a\nLEFT JOIN wx.kudu_wuxiang_src_pho_afterpay_order b ON\n\ta.storeid = b.storeid\ngroup by\n\ta.storeid,\n\ta.openid"
    val ps = conn.prepareStatement(sql)
    val rs: ResultSet = ps.executeQuery
    val col = rs.getMetaData.getColumnCount
    //获取元数据
    val dialect = JdbcDialects.get("jdbc:hive2://192.168.12.204:21050/default;auth=noSasl")
    val schema: StructType = JdbcUtils.getSchema(rs, dialect)
    val rows: Iterator[Row] = JdbcUtils.resultSetToRows(rs, schema)
    import org.json4s._
    implicit val formats = Serialization.formats(ShortTypeHints(List()))
    val data = Extraction.decompose(rows.toList)
    println(schema)
    println(data)
  }

  import java.sql.{DriverManager, SQLException}

  @throws[ClassNotFoundException]
  @throws[SQLException]
  def getConnection: Connection = {
    val driver = "org.apache.hive.jdbc.HiveDriver"
    val url = "jdbc:hive2://192.168.12.204:21050/default;auth=noSasl"
    val username = ""
    val password = ""
    var conn: Connection = null
    Class.forName(driver)
    conn = DriverManager.getConnection(url, username, password)
    conn
  }

  //  def createStructField(name: String, colType: String): StructField = {
  //    colType match {
  //      case "java.lang.String" => {
  //        return StructField(name, StringType, true)
  //      }
  //      case "java.lang.Integer" => {
  //        return StructField(name, IntegerType, true)
  //      }
  //      case "java.lang.Long" => {
  //        return StructField(name, LongType, true)
  //      }
  //      case "java.lang.Boolean" => {
  //        return StructField(name, BooleanType, true)
  //      }
  //      case "java.lang.Double" => {
  //        return StructField(name, DoubleType, true)
  //      }
  //      case "java.lang.Float" => {
  //        return StructField(name, FloatType, true)
  //      }
  //      case "java.sql.Date" => {
  //        return StructField(name, DateType, true)
  //      }
  //      case "java.sql.Time" => {
  //        return StructField(name, TimestampType, true)
  //      }
  //      case "java.sql.Timestamp" => {
  //        return StructField(name, TimestampType, true)
  //      }
  //      case "java.math.BigDecimal" => {
  //        return StructField(name, DecimalType(10, 0), true)
  //      }
  //    }
  //  }
  //
  //  /**
  //    * 把查出的ResultSet转换成DataFrame
  //    */
  //  def createResultSetToDF(rs: ResultSet): DataFrame = {
  //    val rsmd = rs.getMetaData
  //
  //    val columnTypeList: Array[String] = null
  //    val rowSchemaList: Array[StructField] = util.ArrayList[StructField]
  //    for (i <- 1 to rsmd.getColumnCount) {
  //      var temp = rsmd.getColumnClassName(i)
  //      temp = temp.substring(temp.lastIndexOf(".") + 1)
  //      if ("Integer".equals(temp)) {
  //        temp = "Int"
  //      }
  //      columnTypeList.add(temp)
  //      rowSchemaList.add(createStructField(rsmd.getColumnName(i), rsmd.getColumnClassName(i)))
  //    }
  //    val rowSchema = new StructType(rowSchemaList)
  //    //ResultSet反射类对象
  //    val rsClass = rs.getClass
  //    var count = 1
  //    var resultList = new util.ArrayList[Row]
  //    var totalDF = session.createDataFrame(new util.ArrayList[Row], rowSchema)
  //    while (rs.next()) {
  //      count = count + 1
  //      val temp = new util.ArrayList[Object]
  //      for (i <- 0 to columnTypeList.size() - 1) {
  //        val method = rsClass.getMethod("get" + columnTypeList.get(i), "aa".getClass)
  //        temp.add(method.invoke(rs, rsmd.getColumnName(i + 1)))
  //      }
  //      resultList.add(Row(temp: _*))
  //      if (count % 100000 == 0) {
  //        val tempDF = session.createDataFrame(resultList, rowSchema)
  //        totalDF = totalDF.union(tempDF).distinct()
  //        resultList.clear()
  //      }
  //    }
  //    val tempDF = session.createDataFrame(resultList, rowSchema)
  //    totalDF = totalDF.union(tempDF)
  //    return totalDF
  //  }

}

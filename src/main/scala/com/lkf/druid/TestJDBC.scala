package com.lkf.druid

import java.sql.{DriverManager, ResultSet}

import org.apache.spark.{SparkConf, SparkContext}

object TestJDBC {
  var resultSet: ResultSet = null

  //  def main(args: Array[String]): Unit = {
  //    import org.apache.spark.sql.jdbc.JdbcDialects
  //    JdbcDialects.registerDialect(HiveDialect)
  //    val conf = new SparkConf().setAppName("TestJDBC").setMaster("local[2]")
  //
  //    val spark:SparkSession = SparkSession.builder().config(conf).getOrCreate()
  //
  //    try {
  //      for (i <- 1 to 3) {
  //        val connection = () => {
  //          //        Class.forName("org.apache.hive.jdbc.HiveDriver").newInstance()
  //          HiveJdbcPoolUtil.getConnection.get
  //          //        DriverManager.getConnection("jdbc:hive2://192.168.12.204:21050/e000;auth=noSasl", "", "")
  //        }
  //        val sql = "(select\n\tgroup_code,\n\tgroup_name\nfrom\n\te000.dw_crm_member_info_fact\nWHERE\n\tgroup_code = 7292\ngroup by\n\tgroup_code,\n\tgroup_name) AS T"
  //        val jrdd = new JDBCRDD(spark.sparkContext, connection, sql, extractValues)
  //        val caseInsensitiveOptions = CaseInsensitiveMap(
  //          Map("driver" -> "org.apache.hive.jdbc.HiveDriver",
  //            "url" -> "jdbc:hive2://192.168.12.204:21050/e000;auth=noSasl",
  //            "dbtable" -> sql,
  //            "user" -> "",
  //            "password" -> ""))
  //        val jdbcOptions = new JDBCOptions(caseInsensitiveOptions)
  //        val st = resolveTable(jdbcOptions, HiveJdbcPoolUtil.getConnection.get)
  //        println(st)
  //
  //      }
  //
  //      spark.stop()
  //    }
  //    catch {
  //      case e: Exception => println(e.printStackTrace())
  //    }
  //
  //  }
  //
  //  def extractValues(r: ResultSet): Unit = {
  //    resultSet = r
  //  }
  //
  //  def resolveTable(options: JDBCOptions, conn: Connection): StructType = {
  //    val url = options.url
  //    val table = options.table
  //    val dialect = HiveDialect // JdbcDialects.get(url)
  //    //    val conn: Connection = JdbcUtils.createConnectionFactory(options)()
  //    try {
  //      val statement = conn.prepareStatement(dialect.getSchemaQuery(table))
  //      try {
  //        val rs = statement.executeQuery()
  //        try {
  //          JdbcUtils.getSchema(rs, dialect, alwaysNullable = true)
  //        } finally {
  //          rs.close()
  //        }
  //      } finally {
  //        statement.close()
  //      }
  //    } finally {
  //      conn.close()
  //    }
  //  }


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JdbcRDDTest").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sql = "SELECT\n\tstoreid,\n\topenid\nFROM\n\twx.kudu_wuxiang_src_pho_order\ngroup by\n\tstoreid,\n\topenid"
    var txt = sc.textFile("G:\\sougou_user_search_log.txt", 2).map({ line =>
      println(line)
      var fields = line.split("\t")
      for (a <- fields.indices) {
        println(fields(a))
      }
    })
  }

  //获取数据库连接函数
  val getConnection = () => {
    Class.forName("org.apache.hive.jdbc.HiveDriver").newInstance()
    DriverManager.getConnection("jdbc:hive2://192.168.12.204:21050/e000;auth=noSasl", "", "")
  }

  def flatValue(result: ResultSet) = {
    (result.getString(1), result.getString(2))
  }
}


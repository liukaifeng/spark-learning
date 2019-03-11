package com.lkf.v3

import java.sql.{Connection, Statement}

import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types.{DataType, MetadataBuilder}

/**
  * hive jdbc 方言
  **/
object HiveDialect extends JdbcDialect {

  override def canHandle(url: String): Boolean = url.startsWith("jdbc:hive2")


  override def quoteIdentifier(colName: String): String = {
    s"`$colName`"
  }

  /**
    * 拉取数据之前，覆盖连接信息或配置信息，或其他操作
    *
    * @param connection 连接对象
    * @param properties 连接属性
    */
  override def beforeFetch(connection: Connection, properties: Map[String, String]): Unit = {
    val st: Statement = connection.createStatement()
    st.execute("set mem_limit=1G")
  }

  override def getCatalystType(sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    None
  }
}

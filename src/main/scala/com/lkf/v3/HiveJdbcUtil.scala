/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.lkf.v3

import java.sql._

import cn.com.tcsl.cmp.client.dto.report.condition.{DateUtils, SparkSqlCondition}
import cn.com.tcsl.ds.connector.jdbc.impala.HiveQueryResultSet
import com.alibaba.druid.pool.DruidPooledResultSet
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.types.DecimalType.{MAX_PRECISION, MAX_SCALE}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.Array
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.math.min
import scala.util.{Failure, Success}

/**
  * todo 一句话描述该类的用途
  *
  * @author 刘凯峰
  */
object HiveJdbcUtil {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  //hive URL  地址
  private val hiveUrl = "hiveUrl"
  //hive 用户名
  private val hiveUser = "hiveUser"
  //hive 密码
  private val hivePassword = "hivePassword"

  /**
    * 通过hive jdbc 加载数据
    *
    * @param sparkSession      spark 上下文
    * @param sparkSqlCondition 计算条件及hive配置信息
    **/
  def loadData2DataFrame(sparkSession: SparkSession, sparkSqlCondition: SparkSqlCondition, sql: String): DataFrame = {
    import org.apache.spark.sql.jdbc.JdbcDialects
    JdbcDialects.registerDialect(HiveDialect)
    //数据库名称
    var dataBaseName = sparkSqlCondition.getKeyspace
    //hive jdbc配置项
    val hiveJdbcMap = sparkSqlCondition.getHiveJdbcConfig
    // 替换掉数据库前缀
    val preSql = sql.replace(dataBaseName.concat("."), "")
    if (dataBaseName.contains("impala")) {
      dataBaseName = dataBaseName.replace("impala::", "")
    }
    //hive jdbc URL地址
    val hiveJdbcUrl = String.format(hiveJdbcMap.get("hiveUrl"), dataBaseName)
    //hive jdbc table
    val table = String.format("(%s) as T", preSql)
    //加载数据到dataframe
    var df: DataFrame = sparkSession.read
      .format("jdbc")
      .options(Map("driver" -> "org.apache.hive.jdbc.HiveDriver",
        "url" -> hiveJdbcUrl,
        "dbtable" -> table,
        "user" -> hiveJdbcMap.getOrDefault(hiveUser, ""),
        "password" -> hiveJdbcMap.getOrDefault(hivePassword, ""))
      )
      .load()
    df
  }


  def execute2DataFrame(sparkSession: SparkSession, sparkSqlCondition: SparkSqlCondition, sql: String): DataFrame = {
    val map = excecute(sparkSqlCondition, sql)
    val schema = map("schema").asInstanceOf[StructType]
    val rows = map("rowList").asInstanceOf[List[Row]]
    val df: DataFrame = sparkSession.createDataFrame(rows.asJava, schema)
    df
  }

  def execute2Result(sparkSqlCondition: SparkSqlCondition, sql: String): Map[String, Object] = {
    val map = excecute(sparkSqlCondition, sql)
    map
  }

  def excecute(sparkSqlCondition: SparkSqlCondition, sql: String): Map[String, Object] = {
    // 替换掉数据库前缀
    var preSql = sql
    if (preSql.contains("impala::")) {
      preSql = preSql.replace("impala::", "")
    }
    //hive jdbc URL地址
    val hiveJdbcUrl = HiveJdbcPoolUtil.getUrl(sparkSqlCondition.getMongoConfigMap)
    //从连接池获取数据源
    val conn: Connection = HiveJdbcPoolUtil.getConnection.get
    try {
      //执行sql
      val statement = conn.prepareStatement(preSql)
      try {
        //结果集
        val rs: DruidPooledResultSet = statement.executeQuery.asInstanceOf[DruidPooledResultSet]
        //异步记录SQL执行信息
        executionLog(rs, sparkSqlCondition)
        try {
          //根据URL获取对应的jdbc方言
          val dialect = JdbcDialects.get(hiveJdbcUrl)
          //数据集字段属性
          val schema: StructType = getSchema(rs, dialect)
          //数据集row集合
          val rows: List[Row] = JdbcUtils.resultSetToRows(rs, schema).toList
          Map("schema" -> schema, "rowList" -> rows)
        }
        finally {
          rs.close()
        }
      } finally {
        statement.close()
      }
    } finally {
      HiveJdbcPoolUtil.releaseConnection(conn)
    }
  }

  import scala.concurrent.ExecutionContext.Implicits.global

  /**
    * 异步记录 sql 执行统计信息
    *
    * @param sql执行统计信息
    * @param sparkSqlCondition 请求条件
    **/
  def executionLog(rs: DruidPooledResultSet, sparkSqlCondition: SparkSqlCondition) = {
    val f: Future[Boolean] = Future {
      val begin = System.currentTimeMillis()
      val hrs: HiveQueryResultSet = rs.getRawResultSet.asInstanceOf[HiveQueryResultSet]
      val profile: String = hrs.getRuntimeProfie
      val executionProfile: SqlExecutionProfile = new SqlExecutionProfile
      executionProfile.tracId = sparkSqlCondition.getTracId
      executionProfile.createTime = DateUtils.convertTimeToString(begin, DateUtils.MILLS_SECOND_OF_DATE_FRM)
      executionProfile.summary = profile
      executionProfile.executionType = "report"
      InterpreterUtil.saveSqlExtLog(executionProfile, sparkSqlCondition, SqlExecutionEnum.SQL_SUMMARY.toString)
      true
    }
    f onComplete {
      case Success(result) => logger.info("==========Async log record success===========")
      case Failure(t) => logger.error("An error has occured: " + t.getMessage)
    }
  }

  private def getSchema(
                         resultSet: ResultSet,
                         dialect: JdbcDialect,
                         alwaysNullable: Boolean = false): StructType = {
    val rsmd = resultSet.getMetaData
    val ncols = rsmd.getColumnCount
    val fields = new Array[StructField](ncols)
    var i = 0
    while (i < ncols) {
      val columnName = rsmd.getColumnLabel(i + 1)
      val dataType = rsmd.getColumnType(i + 1)
      val typeName = rsmd.getColumnTypeName(i + 1)
      val fieldSize = rsmd.getPrecision(i + 1)
      val fieldScale = rsmd.getScale(i + 1)
      val isSigned = true
      val nullable = if (alwaysNullable) {
        true
      } else {
        rsmd.isNullable(i + 1) != ResultSetMetaData.columnNoNulls
      }
      val metadata = new MetadataBuilder().putLong("scale", fieldScale)
      val columnType =
        dialect.getCatalystType(dataType, typeName, fieldSize, metadata).getOrElse(
          getCatalystType(dataType, fieldSize, fieldScale, isSigned))
      fields(i) = StructField(columnName, columnType, nullable)
      i = i + 1
    }
    new StructType(fields)
  }

  /**
    * Maps a JDBC type to a Catalyst type.  This function is called only when
    * the JdbcDialect class corresponding to your database driver returns null.
    *
    * @param sqlType - A field of java.sql.Types
    * @return The Catalyst type corresponding to sqlType.
    */
  private def getCatalystType(
                               sqlType: Int,
                               precision: Int,
                               scale: Int,
                               signed: Boolean): DataType = {
    val answer = sqlType match {
      // scalastyle:off
      case java.sql.Types.ARRAY => null
      case java.sql.Types.BIGINT => if (signed) {
        LongType
      } else {
        DecimalType(20, 0)
      }
      case java.sql.Types.BINARY => BinaryType
      case java.sql.Types.BIT => BooleanType // @see JdbcDialect for quirks
      case java.sql.Types.BLOB => BinaryType
      case java.sql.Types.BOOLEAN => BooleanType
      case java.sql.Types.CHAR => StringType
      case java.sql.Types.CLOB => StringType
      case java.sql.Types.DATALINK => null
      case java.sql.Types.DATE => DateType
      case java.sql.Types.DECIMAL
        if precision != 0 || scale != 0 => bounded(precision, scale)
      case java.sql.Types.DECIMAL => DecimalType.SYSTEM_DEFAULT
      case java.sql.Types.DISTINCT => null
      case java.sql.Types.DOUBLE => DoubleType
      case java.sql.Types.FLOAT => FloatType
      case java.sql.Types.INTEGER => if (signed) {
        IntegerType
      } else {
        LongType
      }
      case java.sql.Types.JAVA_OBJECT => null
      case java.sql.Types.LONGNVARCHAR => StringType
      case java.sql.Types.LONGVARBINARY => BinaryType
      case java.sql.Types.LONGVARCHAR => StringType
      case java.sql.Types.NCHAR => StringType
      case java.sql.Types.NCLOB => StringType
      case java.sql.Types.NULL => null
      case java.sql.Types.NUMERIC
        if precision != 0 || scale != 0 => bounded(precision, scale)
      case java.sql.Types.NUMERIC => DecimalType.SYSTEM_DEFAULT
      case java.sql.Types.NVARCHAR => StringType
      case java.sql.Types.OTHER => null
      case java.sql.Types.REAL => DoubleType
      case java.sql.Types.REF => StringType
      case java.sql.Types.REF_CURSOR => null
      case java.sql.Types.ROWID => LongType
      case java.sql.Types.SMALLINT => IntegerType
      case java.sql.Types.SQLXML => StringType
      case java.sql.Types.STRUCT => StringType
      case java.sql.Types.TIME => TimestampType
      case java.sql.Types.TIME_WITH_TIMEZONE
      => null
      case java.sql.Types.TIMESTAMP => TimestampType
      case java.sql.Types.TIMESTAMP_WITH_TIMEZONE
      => null
      case java.sql.Types.TINYINT => IntegerType
      case java.sql.Types.VARBINARY => BinaryType
      case java.sql.Types.VARCHAR => StringType
      case _ =>
        throw new SQLException("Unrecognized SQL type " + sqlType)
      // scalastyle:on
    }

    if (answer == null) {
      throw new SQLException("Unsupported type " + JDBCType.valueOf(sqlType).getName)
    }
    answer
  }

  private def bounded(precision: Int, scale: Int): DecimalType = {
    DecimalType(min(precision, MAX_PRECISION), min(scale, MAX_SCALE))
  }
}

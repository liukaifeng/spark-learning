package com.lkf.v3

import java.sql.Connection
import java.util.Properties
import javax.sql.DataSource

import com.alibaba.druid.pool.{DruidDataSource, DruidDataSourceFactory}
import org.slf4j.{Logger, LoggerFactory}


object HiveJdbcPoolUtil {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  var url: String = _
  val druidProps = new Properties()

  import org.apache.spark.sql.jdbc.JdbcDialects

  JdbcDialects.registerDialect(HiveDialect)

  val dataSource: Option[DataSource] = {
    logger.info("----------------Connection pool initialization,reading configuration file--------------------")
    try {
      val druidProps = new Properties()
      // 获取Druid连接池的配置文件
      val druidConfig = getClass.getResourceAsStream("/druid.properties")
      // 倒入配置文件
      druidProps.load(druidConfig)
      url = druidProps.getProperty("url")
      Some(DruidDataSourceFactory.createDataSource(druidProps))
    } catch {
      case error: Exception =>
        None
    }
  }

  // 连接方式
  def getConnection: Option[Connection] = {
    logger.info("----------------Get the connection object--------------")
    dataSource match {
      case Some(ds) => Some(ds.getConnection())
      case None => None
    }
  }

  /**
    * 释放连接
    *
    * @param conn 待释放的连接对象
    **/
  def releaseConnection(conn: Connection): Unit = {
    if (!conn.isClosed) {
      logger.info("-----------------Release connection object-----------------")
      logger.info("-----------------Monitor:{}-----------------", JsonUtil.objectToJson(getMonitorInfo))
      conn.close()
    }
  }

  /**
    * 获取连接池的监控信息
    *
    **/
  def getMonitorInfo: MonitorInfo = {
    val druidDataSource = dataSource.get.asInstanceOf[DruidDataSource]
    new MonitorInfo(druidDataSource.getConnectCount, druidDataSource.getActiveCount, druidDataSource.getCreateCount, druidDataSource.getCloseCount)
  }
}

class MonitorInfo(connCount: Long, actCount: Long, crtCount: Long, clsCount: Long) {
  var connectCount: Long = connCount
  var activeCount: Long = actCount
  var createCount: Long = crtCount
  var closeCount: Long = clsCount
}
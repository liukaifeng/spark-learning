package com.lkf.v3

import java.sql.Connection
import java.util.Properties

import com.alibaba.druid.pool.{DruidDataSource, DruidDataSourceFactory}
import com.google.gson.reflect.TypeToken
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.{DBCollection, DBObject}
import javax.sql.DataSource
import org.slf4j.{Logger, LoggerFactory}

/**
  * 连接池管理对象
  **/
object HiveJdbcPoolUtil {
  //初始化日志对象
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  //数据库连接地址
  private var url: String = _
  //数据源
  var dataSource: Option[DataSource] = Option.empty
  //配置信息
  var confMap: Properties = _

  import org.apache.spark.sql.jdbc.JdbcDialects
  //注册jdbc方言
  JdbcDialects.registerDialect(HiveDialect)

  //获取数据库url地址
  def getUrl(mongoConf: java.util.Map[String, String]): String = {
    if (url == null || url.isEmpty) {
      init(mongoConf)
    }
    url
  }

  /**
    * 读取配置文件，初始化数据源
    **/
  def init(mongoConf: java.util.Map[String, String]): String = {
    try {
      //解析配置文件
      val druidProps = parseConf(mongoConf)
      //获取数据库连接地址
      url = druidProps.getProperty("url")
      //创建数据源
      dataSource = Some(DruidDataSourceFactory.createDataSource(druidProps))
    } catch {
      case ex: Exception =>
        logger.error("初始化数据源异常：{}", ex.toString)
        None
    }
    url
  }

  /**
    * 从 mongodb 获取配置信息
    *
    * @param mongoConf MongoDB配置
    **/
  def parseConf(mongoConf: java.util.Map[String, String]): Properties = {
    //获取 mongo 客户端
    val mongoClient = InterpreterUtil.getMongoClient(mongoConf)
    //获取表对象
    val collection: DBCollection = mongoClient.getDB(mongoConf.get("mongoDb")).getCollection("sys_envi")
    //查询条件
    val dbObject: DBObject = collection.findOne(MongoDBObject("application" -> "loongboss", "profile" -> "runsoul"))
    import com.google.gson.Gson
    //实例化json对象
    val gson = new Gson
    //序列化配置
    val confJson = JsonUtil.objectToJson(dbObject.get("conf"))
    //将配置项转换为 Properties
    val typeToken = new TypeToken[Properties]() {}.getType
    confMap = gson.fromJson(confJson, typeToken)
    confMap
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
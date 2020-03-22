package com.lkf.v3

import cn.com.tcsl.cmp.client.dto.report.condition.SparkSqlCondition
import com.mongodb.casbah.{MongoClient, MongoCredential}
import com.mongodb.util.JSON
import com.mongodb.{DBCollection, DBObject, ServerAddress}
import org.slf4j.LoggerFactory

/**
 * 拦截器公共方法
 *
 * @author kaifeng
 * @date 2019/2/25
 */
object InterpreterUtil {
  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * 保存日志到mongodb
   *
   * @param log               日志对象
   * @param sparkSqlCondition 请求条件
   * @param tb                collection 名称
   **/
  def saveSqlExtLog[T <: Any](log: T, sparkSqlCondition: SparkSqlCondition, tb: String): Unit = {
    try {
      val mongoClient = getMongoClient(sparkSqlCondition.getMongoConfigMap)
      val collection: DBCollection = mongoClient.getDB(sparkSqlCondition.getMongoConfigMap.get("mongoDb")).getCollection(tb)
      import com.google.gson.Gson
      val gson = new Gson
      val dbObject: DBObject = JSON.parse(gson.toJson(log)).asInstanceOf[DBObject]
      collection.insert(dbObject)
    } catch {
      case exception: Exception =>
        println("【SQLExtInterpreter::saveSqlExtLog】-MongoDB记录日志出现异常")
    }
  }

  /**
   * 保存日志到mongodb
   *
   * @param log               日志对象
   * @param sparkSqlCondition 请求条件
   **/
  def saveSqlExtLog[T <: Any](log: T, sparkSqlCondition: SparkSqlCondition): Unit = {
    saveSqlExtLog(log, sparkSqlCondition, SqlExecutionEnum.SQL_REPORT.toString)
  }

  def getMongoClient(mongoMap: java.util.Map[String,String]): MongoClient = {
    val mongoClient: MongoClient = this.getMongoClientCredential(mongoMap.get("mongoHost"),
      Integer.valueOf(mongoMap.get("mongoPort")),
      mongoMap.get("mongoDb"),
      mongoMap.get("mongoUserName"),
      mongoMap.get("mongoPassword"))
    mongoClient
  }

  /**
   * 连接mongodb
   *
   * @param ip        ip地址
   * @param port      端口号
   * @param dbName    数据库名
   * @param loginName 用户名
   * @param password  密码
   **/
  def getMongoClientCredential(ip: String, port: Int, dbName: String, loginName: String, password: String): MongoClient = {
    val server = new ServerAddress(ip, port)
    //注意：MongoCredential中有6种创建连接方式，这里使用MONGODB_CR机制进行连接。如果选择错误则会发生权限验证失败
    val credentials = MongoCredential.createScramSha1Credential(loginName, dbName, password.toCharArray)
    val mongoClient = MongoClient(server, List(credentials))
    mongoClient
  }


}

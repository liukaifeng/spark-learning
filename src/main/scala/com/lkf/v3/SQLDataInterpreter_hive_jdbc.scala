package com.lkf.v3

import java.lang.reflect.InvocationTargetException
import java.util

import org.apache.livy.client.ext.model.Constant.FunctionType
import org.apache.livy.client.ext.model.{Constant, SparkSqlBuild, SparkSqlCondition}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, Row, SparkSession}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.parse
import org.json4s.{DefaultFormats, Extraction, JObject, _}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.{JavaConversions, mutable}
import scala.util.control.NonFatal

object SQLDataInterpreter_hive_jdbc {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private implicit def formats = DefaultFormats

  //对比项分隔符
  private val compareSplitChar = Constant.COMPARE_SPLIT_CHAR

  private val APPLICATION_JSON = "application/json"

  def main(args: Array[String]): Unit = {
    val code = "{\"accessToken\":\"e0c601ce-413d-479b-bbb8-ae6b800094b3\",\"computeKind\":\"sql_ext_dc\",\"dataSourceType\":0,\"hiveJdbcConfig\":{\"hivePassword\":\"\",\"hiveUrl\":\"jdbc:hive2://192.168.12.204:21050/%s;auth=noSasl\",\"hiveUser\":\"\"},\"kuduMaster\":\"hadoop207\",\"limit\":0,\"maxWaitSeconds\":60,\"mongoConfig\":{\"mongoHost\":\"192.168.12.117\",\"mongoUserName\":\"lb\",\"mongoPort\":\"30017\",\"mongoPassword\":\"Lb#827\",\"mongoDb\":\"lb\"},\"page\":0,\"platformVersion\":\"0\",\"queryPoint\":0,\"queryType\":0,\"sessionGroup\":\"group_report\",\"sessionId\":\"2\",\"sparkConfig\":{\"spark.executor.memory\":\"1024m\",\"spark.executor.cores\":\"1\",\"groupName\":\"group_report\",\"spark.driver.cores\":\"1\",\"spark.scheduler.mode\":\"FAIR\",\"spark.sql.shuffle.partitions\":\"1\",\"spark.driver.memory\":\"1024m\",\"spark.executor.instances\":\"1\",\"spark.custom.coalesce\":\"1\",\"spark.default.parallelism\":\"1\"},\"sql\":\"select   cast(di1lie as String) as `ljc_group_x_di1lie1545713704000_0` , di2lie as `ljc_group_x_di2lie1545355257000_0` , from_timestamp(di6lie,'yyyy-MM-dd') as `ljc_group_x_di6lie1545713692000_0` , cast(di8lie as String) as `ljc_compare_x_di8lie1545355263000_0` ,sum(CAST(di12lie AS DOUBLE)) as `ljc_sum_x_di12lie1545355305000_0` from e000002.shujubiaoquanxiantest_jkn_my_000002 group by `ljc_group_x_di1lie1545713704000_0`,`ljc_group_x_di2lie1545355257000_0`,`ljc_group_x_di6lie1545713692000_0`,`ljc_compare_x_di8lie1545355263000_0` limit 100\",\"synSubmit\":true,\"tracId\":\"CP__4823622813205127058\"}"
    val sqlExtLog: SqlExtInterpreterLog = new SqlExtInterpreterLog
    var result: JObject = null
    //组装spark sql
    val conditionParam: SparkSqlCondition = new SparkSqlBuild().getDataCubeExecuteCondition(code)
    try {
      if (conditionParam.getCompareList.isEmpty) {
        result = normalExecute(conditionParam)
      }
      else {
        result = pivotExecute(conditionParam)
      }
      print(result)
      //      Interpreter.ExecuteSuccess(result)
    } catch {
      case e: InvocationTargetException =>
        val cause = e.getTargetException
        logger.error(s"Fail to execute query $code", cause)
        sqlExtLog.msg = cause.toString
      //        Interpreter.ExecuteError("Error", cause.getMessage, cause.getStackTrace.map(_.toString))
      case NonFatal(f) =>
        logger.error(s"Fail to execute query $code", f)
        sqlExtLog.msg = f.toString
      //        Interpreter.ExecuteError("Error", f.getMessage, f.getStackTrace.map(_.toString))
    }
    finally {

    }

  }


  /**
    * 普通计算
    *
    * @param condition 参数
    **/
  def normalExecute(condition: SparkSqlCondition): JObject = {
    val map = HiveJdbcUtil.execute2Result(condition, condition.getSelectSql)
    val schema = map("schema").asInstanceOf[StructType]
    val rows = map("rowList").asInstanceOf[List[Row]]
    val jSchema = parse(schema.json)
    val jRows = Extraction.decompose(rows.toArray.map(_.toSeq))
    APPLICATION_JSON -> (("schema" -> jSchema) ~ ("data" -> jRows) ~ ("total" -> 0))
  }

  /**
    * 列转行计算
    *
    * @param condition 参数
    **/
  def pivotExecute(condition: SparkSqlCondition): JObject = {
    val sparkSession: SparkSession = SparkSourceContext.getSparkSession(condition)
    var df = HiveJdbcUtil.execute2DataFrame(sparkSession, condition, condition.getSelectSql)
    df.show()
    val sparkAggMap: mutable.Map[String, util.List[String]] = condition.getSparkAggMap.asScala
    val aggList: List[Column] = sparkAgg(sparkAggMap)
    val compareList: List[String] = if (condition.getCompareList != null) JavaConversions.asScalaBuffer(condition.getCompareList).toList else Nil
    val groupList: List[String] = if (condition.getGroupList != null) JavaConversions.asScalaBuffer(condition.getGroupList).toList else Nil
    if (aggList != null && aggList.nonEmpty) {
      if (groupList != null && groupList.nonEmpty) {
        df = df.groupBy(groupList.head, groupList.tail: _*).pivot(compareList.head).agg(aggList.head, aggList.tail: _*)
      }
      else {
        df = df.groupBy().pivot(compareList.head).agg(aggList.head, aggList.tail: _*)
      }
    }
    df.show()
    val jsonString = df.schema.json
    val jSchema = parse(jsonString)
    // 11-获取数据
    val rows = df.take(10)
    val jRows = Extraction.decompose(rows.map(_.toSeq))
    APPLICATION_JSON -> (("schema" -> jSchema) ~ ("data" -> jRows) ~ ("total" -> 0))
  }

  def sparkAgg(aggMap: mutable.Map[String, util.List[String]]): List[Column] = {
    var aggColumnList: List[Column] = Nil
    if (aggMap != null && aggMap.nonEmpty) {
      for (key <- aggMap.keys) {
        val aggs: mutable.Buffer[String] = aggMap(key).asScala
        for (agg <- aggs) {
          if (key == FunctionType.FUNC_SUM.getCode) {
            aggColumnList = aggColumnList :+ sum(agg).alias(agg)
          }
          if (key == FunctionType.FUNC_COUNT.getCode) {
            aggColumnList = aggColumnList :+ count(agg).alias(agg)
          }
          if (key == FunctionType.FUNC_AVG.getCode) {
            aggColumnList = aggColumnList :+ avg(agg).alias(agg)
          }
        }
      }
    }
    aggColumnList
  }
}

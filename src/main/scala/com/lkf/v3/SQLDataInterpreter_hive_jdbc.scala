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
    val code = "{\"accessToken\":\"b1241c12-b194-464e-9f36-e3ecbf904e9d\",\"sql\":\"SELECT   store_name AS store_name,   CAST(group_name AS STRING) AS group_name,   SUM(dinner_time) AS dinner_time FROM   e000.dw_trade_bill_fact_p_group_n2 WHERE group_code = 9759 GROUP BY store_name,   group_name ORDER BY dinner_time NULLS LAST LIMIT 1500\",\"compareCondition\":[{\"aliasName\":\"\",\"dataType\":\"\",\"fieldAliasName\":\"group_name\",\"fieldDescription\":\"\",\"fieldGroup\":0,\"fieldId\":\"180620140418000825\",\"fieldName\":\"group_name\",\"isBuildAggregated\":0,\"originDataType\":\"\",\"udfType\":0,\"uniqId\":\"\"}],\"dataSourceType\":1,\"dbName\":\"impala::e000\",\"dimensionCondition\":[{\"aliasName\":\"\",\"dataType\":\"\",\"fieldAliasName\":\"store_name\",\"fieldDescription\":\"\",\"fieldGroup\":0,\"fieldId\":\"\",\"fieldName\":\"store_name\",\"isBuildAggregated\":0,\"originDataType\":\"\",\"udfType\":0,\"uniqId\":\"\"}],\"filterCondition\":[],\"hiveJdbcConfig\":{\"hiveUrl\":\"jdbc:hive2://192.168.12.204:21050/%s;auth=noSasl\",\"hiveUser\":\"\",\"hivePassword\":\"\"},\"indexCondition\":[{\"aggregator\":\"sum\",\"aggregatorName\":\"\",\"aliasName\":\"\",\"dataType\":\"\",\"fieldAliasName\":\"dinner_time\",\"fieldDescription\":\"\",\"fieldGroup\":0,\"fieldId\":\"\",\"fieldName\":\"dinner_time\",\"isBuildAggregated\":0,\"originDataType\":\"\",\"qoqType\":0,\"udfType\":0,\"uniqId\":\"\"}],\"kuduMaster\":\"hadoop207\",\"limit\":10,\"maxWaitSeconds\":60,\"mongoConfig\":{\"mongoHost\":\"192.168.12.117\",\"mongoUserName\":\"lb\",\"mongoPort\":\"30017\",\"mongoPassword\":\"Lb#827\",\"mongoDb\":\"lb\"},\"page\":0,\"platformVersion\":\"0\",\"queryPoint\":1,\"queryType\":0,\"reportCode\":\"181029104703005073\",\"sessionGroup\":\"group_report\",\"sessionId\":\"0\",\"sortCondition\":[],\"sparkConfig\":{\"groupName\":\"group_report\",\"spark.default.parallelism\":\"20\",\"spark.sql.shuffle.partitions\":\"20\",\"spark.executor.instances\":\"2\",\"spark.executor.cores\":\"1\",\"spark.driver.cores\":\"1\",\"spark.driver.memory\":\"2000m\",\"spark.executor.memory\":\"2000m\",\"spark.scheduler.mode\":\"FAIR\",\"spark.custom.coalesce\":\"1\"},\"synSubmit\":true,\"tbId\":\"180620140417000008\",\"tbName\":\"dw_trade_bill_fact_p_group_n2\",\"tracId\":\"1551063485000\"}"
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
    val jsonString = df.schema.json
    val jSchema = parse(jsonString)
    // 11-获取数据
    val rows = df.take(condition.getLimit).map(_.toSeq)
    val jRows = Extraction.decompose(rows)
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

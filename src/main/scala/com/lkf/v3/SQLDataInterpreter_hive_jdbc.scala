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
    val code = "{\"accessToken\":\"datacube-open-apis-1111-666666666666\",\"compareCondition\":[{\"fieldAliasName\":\"payway\",\"fieldGroup\":0,\"fieldName\":\"payway\",\"isBuildAggregated\":0,\"udfType\":0}],\"computeKind\":\"sql_ext_dc\",\"dataSourceType\":1,\"dimensionCondition\":[{\"fieldAliasName\":\"store_name\",\"fieldGroup\":0,\"fieldName\":\"store_name\",\"isBuildAggregated\":0,\"udfType\":0}],\"hiveJdbcConfig\":{\"hiveUrl\":\"jdbc:hive2://192.168.12.204:21050/%s;auth=noSasl\",\"hiveUser\":\"\",\"hivePassword\":\"\"},\"indexCondition\":[{\"aggregator\":\"sum\",\"fieldAliasName\":\"notincome\",\"fieldGroup\":0,\"fieldName\":\"notincome\",\"isBuildAggregated\":0,\"qoqType\":0,\"udfType\":0}],\"kuduMaster\":\"hadoop207\",\"limit\":0,\"maxWaitSeconds\":60,\"mongoConfig\":{\"mongoHost\":\"192.168.12.117\",\"mongoUserName\":\"lb\",\"mongoPort\":\"30017\",\"mongoPassword\":\"Lb#827\",\"mongoDb\":\"lb\"},\"page\":0,\"platformVersion\":\"0\",\"queryPoint\":0,\"queryType\":0,\"sessionGroup\":\"group_report\",\"sessionId\":\"4\",\"sparkConfig\":{\"groupName\":\"group_report\",\"spark.default.parallelism\":\"20\",\"spark.sql.shuffle.partitions\":\"20\",\"spark.executor.instances\":\"2\",\"spark.executor.cores\":\"2\",\"spark.driver.cores\":\"1\",\"spark.driver.memory\":\"2000m\",\"spark.executor.memory\":\"2000m\",\"spark.scheduler.mode\":\"FAIR\",\"spark.custom.coalesce\":\"1\"},\"sql\":\"select  store_name, replace(settlement_name, '.', '$') as payway, cast(round(sum(non_income),2) as decimal(38,2)) as notIncome from e000.dw_trade_settle_detail_fact_p_group_upgrade  where group_code = 3297 and table_settle_time >= '2018-01-01 00:00:00' and table_settle_time < '2018-03-01 00:00:00' group by store_name,settlement_name order by store_name,settlement_name limit 1500 \",\"synSubmit\":true,\"tracId\":\"00001\"}"
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

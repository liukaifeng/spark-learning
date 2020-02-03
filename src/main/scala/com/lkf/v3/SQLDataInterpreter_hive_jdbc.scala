package com.lkf.v3

import java.lang.reflect.InvocationTargetException
import java.util

import cn.com.tcsl.cmp.client.dto.report.condition.Constant.FunctionType
import cn.com.tcsl.cmp.client.dto.report.condition.{Constant, SparkSqlBuild, SparkSqlCondition}
import org.apache.livy.client.common.ext.SparkExtParam
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
    val code = "{\"accessToken\":\"datacube-open-apis-1111-666666666666\",\"compareCondition\":[{\"fieldAliasName\":\"payway\",\"fieldGroup\":0,\"fieldName\":\"payway\",\"isBuildAggregated\":0,\"nanFlag\":0,\"udfType\":0}],\"computeKind\":\"sql_ext_dc\",\"dataSourceType\":1,\"dimensionCondition\":[{\"fieldAliasName\":\"rectype\",\"fieldGroup\":0,\"fieldName\":\"rectype\",\"isBuildAggregated\":0,\"nanFlag\":0,\"udfType\":0}],\"indexCondition\":[{\"aggregator\":\"sum\",\"fieldAliasName\":\"money\",\"fieldGroup\":0,\"fieldName\":\"money\",\"isBuildAggregated\":0,\"nanFlag\":0,\"qoqType\":0,\"udfType\":0}],\"limit\":0,\"maxWaitSeconds\":60,\"page\":0,\"platformVersion\":\"0\",\"queryPoint\":0,\"queryType\":0,\"sessionGroup\":\"group_report\",\"sql\":\"select   store_name \\\"recType\\\",     replace(settlement_name, '.', '$') \\\"payway\\\", round(sum(income),2) \\\"money\\\"/**纯收金额*/ from e000.dw_trade_settle_detail_fact_p_group_upgrade  where group_code = 3297 and store_code in ( 3298,3404,3472,3473,3474,3475,3476,3477,3478,3479,3480,3681,3708,3718,3824,3825,3826,3827,3933,5040,5050,5081,5189,5190,5191,5192,5193,5194,5195,5196,5197,5198,5217,5218,5219,5220,5221,5222,5223,5224,5225,5226,5227,5228,5229,5230,5231,5838,5860,5861,8187,9834,9835,9836,9837,9838,10033,10106,11180,11181,11182,11183,11184,11494,11760,11761,11762,11763,11923,11924,12612,12730,12776,12808,12998,12999,13000,13001,13002,13042,13174,13176,13177,13178,13179,13180,13219,13220,13268,13550,13756,13830,13831,13832,13999,14012,14145,14217,14218,14219,14302,14340,14527,14689,14690,14865,14867,14925,14977,14978,14979,14980,15143,15766,16569,16579,16714,16717,16998,17026,17155,17271,17308,17312,17374,17375,17500,17501,17502,18208,18209,18437,18439,18440,18686,18687,18688,18754,18777,18841,18842,18883,18971,19088,19089,19169,19388,19490,19492,19493,19575,19585,19645,19781,19944,19981,19982,19983,19984,20504,20505,20506,20507,20540,20541,20631,20786,20787,20788,20909,20910,20913,20914,21258,21259,21494,21495,21496,21498,21499,21502,21503,21631,21632,21633,21634,21844,21845,21846,21847,22335,22336,22337,22338,22339,22340,22341,22342,22343,22344,22345,22346,22347,22348,22362,22657,22658,22659,22660,22661,22662,22778,22779,22878,22879,22880,22881,22882,22883,22884,23082,23083,23084,23085,23086,23087,23088,23370,23371,23372,23373,23374,23836,23837,23838,23839,23840,23841,23842,23843,23844,23845,23846,24338,24369,24370,24371,24372,24373,24374,24375,24376,24377,24378,24379,24380,24381,24382,24383,24384,24385,24386,24726,24736,24737,24738,25082,25083,25084,25085,25086,25453,25454,25455,25456,25457,25458,25459,25460,25461,25594,25635,25750,25751,25752,26121,26122,26123,26126,26127,26721,26722,26723,26724,26725,26726,26732,26796,26797,26798,27047,27048,27049,27050,27051,27055,27413,27414,27415,27416,27417,27421,27422,27668,27678,27679,27680,27681,27682,27683,27684,27685,27691,28197,28198,28199,28200,28201,28202,28203,28204,28205,28206,28207,28208,28746,28784,28785,28786,28787,28788,28789,28790,28791,29121,29122,29123,29124,29125,29149,29150,29151,29152,29153,29397,29398,29399,29400,29401,29402,29403,29404,29647,29648,29649,29650,29762,29763,29764,29765,29766,30150,30151,30152,30153,30154,30155,30156,30157,30158,30159,30160,30161,30162,30163,30164,30165,30171,30220,30221,30222,30223,30224,30390,30391,30392,30590,30591,31539,31540,31541,31542,31543,32154,32155,32156,32157,32158,32357,32360,32361,32362,32748,32749,32750,32751,32752,32753,33308,33322,33323,33324,33325,33326,33430,33431,33432,33433,33434,33435,33436,33917,33918,33919,33920,33921,33922,34364,34365,34366,34367,34665,34666,34667,34668,34669,34847,34848,34849,34850,34851,34852,34853,34978,34979,34989,34990,34991,34992,34993,35211,35361,35362,35372,35397,35492,35493,35494,35495,35496,35517,35555,35585,35716,35717,35788,35789,35830,36006,36007,36073,36147,36148,36149,36150,36151,36152,36153,36360,36361,36445,36446,36447,36467,36493,36635,36636,36637,36638,36639,36786,36787,36855,36999,37027,37028,37029,37030,37031,37032,37033,37034,37035,37092,37093,37261,37262,37263,37264,37265,37470,37471,37472,37473,37474,37475,37476,37477,37478,37480,37481,37482,37483,37484,37757,37758,37759,37760,37761,37762,37763,37765,37766,37904,37905,37906,37907,37908,37972,37973,37974,37975,37976,38061,38090,38359,38360,38361,38362,38363,38364,38365,38366,38367,38488,38489,38490,38526,38730,38731,38732,38733,38734,38735,38736,38875,38876,38877,38878,38879,38899,39083,39084,39085,39086,39087,39230,39231,39232,39233,39284,39285,39286,39287,39308,39309,39329,39355,39486,39487,39488,39493,39537,39538,39539,39730,39731,39732,39771,39944,39945,40142,40143,40144,40145,40146,40277,40338,40339,40340,40341,40426,40427,40469,40470,40471,40472,40473,40539,40540,40541,40542,40543,40544,40545,40546,40547,40548,40549,40550,40661,40662,40663,40664,40665,40666,40812,40832,41053,41054,41055,41056,41057,41058,41059,41060,41219,41220,41221,41222,41223,41363,41364,41365,41366,41367,41368,41369,41498,41499,41500,41501,41502,41503,41571,41572,41611,41612,41613,41709,41710,41711) and manage_type in ('直营','加盟','托管') and table_settle_time >= '2019-08-01 06:00:00' and table_settle_time < '2019-09-01 06:00:00' and income > 0  and store_name is not null group by store_name, settlement_name order by store_name, settlement_name limit 1500 \",\"synSubmit\":true,\"tracId\":\"CP__608032765976783328\"}"
    val sqlExtLog: SqlExtInterpreterLog = new SqlExtInterpreterLog
    var result: JObject = null
    //组装spark sql
    val conditionParam: SparkSqlCondition =  new SparkExtParam().analyzeParam(code)
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
    //parse(schema.json)
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

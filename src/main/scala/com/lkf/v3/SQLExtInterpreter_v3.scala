package com.lkf.v3

import java.io.StringWriter
import java.lang.reflect.InvocationTargetException
import java.util
import java.util.Objects

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.mongodb.casbah.{MongoClient, MongoCredential}
import com.mongodb.util.JSON
import com.mongodb.{DBCollection, DBObject, ServerAddress}
import org.apache.kudu.spark.kudu._
import org.apache.livy.client.ext.model.Constant.PIVOT_ALIAS
import org.apache.livy.client.ext.model.{DateUtils, QoqDTO, SparkSqlCondition, _}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.{JavaConversions, mutable}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object SQLExtInterpreter_v3 {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private implicit def formats = DefaultFormats

  //对比项分隔符
  private val compareSplitChar = Constant.COMPARE_SPLIT_CHAR

  //逗号分隔符
  private val splitDot = ","

  //默认返回结果集条数
  private var defaultLimit = 1500

  private val fillValue = -123

  //对比列值数量限制
  private val compareLimit = 1000

  //默认分区数量
  private var defaultNumPartitions = 1

  private val unknown = "未知"

  private val joinTypeInner = "left"
  private val joinTypeCross = "cross"

  def main(args: Array[String]): Unit = {
    val sqlExtLog: SqlExtInterpreterLog_v3 = new SqlExtInterpreterLog_v3
    val beginTime: Long = System.currentTimeMillis()
    sqlExtLog.beginTime = DateUtils.convertTimeToString(beginTime, DateUtils.MILLS_SECOND_OF_DATE_FRM)
    System.setProperty("hadoop.home.dir", "D:\\Java\\hadoop-3.0")

    val param = "{\"compareCondition\":[],\"dataSourceType\":1,\"dbName\":\"impala::e000\",\"dimensionCondition\":[{\"aliasName\":\"客位名称\",\"dataType\":\"str\",\"fieldAliasName\":\"ljc_group_x_table_name_0\",\"fieldDescription\":\"客位名称\",\"fieldGroup\":0,\"fieldId\":\"180620140417000697\",\"fieldName\":\"table_name\",\"isBuildAggregated\":0,\"udfType\":0,\"uniqId\":\"1545103298000\"},{\"aliasName\":\"营业日\",\"dataType\":\"datetime\",\"fieldAliasName\":\"ljc_group_x_settle_biz_date_0\",\"fieldDescription\":\"营业日\",\"fieldGroup\":0,\"fieldId\":\"180620140417000718\",\"fieldName\":\"settle_biz_date\",\"isBuildAggregated\":0,\"udfType\":0,\"uniqId\":\"1545103336000\"},{\"aliasName\":\"就餐人数\",\"dataType\":\"int\",\"fieldAliasName\":\"ljc_group_x_people_qty_0\",\"fieldDescription\":\"就餐人数\",\"fieldGroup\":0,\"fieldId\":\"180620140417000705\",\"fieldName\":\"people_qty\",\"isBuildAggregated\":0,\"udfType\":0,\"uniqId\":\"1545103358000\"}],\"enterTime\":1545104573925,\"filterCondition\":[{\"dataType\":\"str\",\"fieldAliasName\":\"ljc_filter_x_group_code_0\",\"fieldDescription\":\"集团\",\"fieldGroup\":0,\"fieldName\":\"group_code\",\"fieldValue\":[\"9759\"],\"isBuildAggregated\":0,\"udfType\":0},{\"aggregator\":\"\",\"aggregatorName\":\"每月\",\"dataType\":\"datetime\",\"fieldAliasName\":\"ljc_filter_x_settle_biz_dateevery_month_0\",\"fieldDescription\":\"营业日\",\"fieldFormula\":\"\",\"fieldGroup\":0,\"fieldId\":\"180620140417000718\",\"fieldName\":\"settle_biz_date\",\"fieldValue\":[\"10\",\"30\"],\"granularity\":\"every_month\",\"isBuildAggregated\":0,\"udfType\":0},{\"aggregator\":\"lte\",\"aggregatorName\":\"小于等于\",\"dataType\":\"int\",\"fieldAliasName\":\"ljc_group_x_people_qty_0\",\"fieldDescription\":\"就餐人数\",\"fieldFormula\":\"\",\"fieldGroup\":0,\"fieldId\":\"180620140417000705\",\"fieldName\":\"people_qty\",\"fieldValue\":[\"5\"],\"granularity\":\"\",\"isBuildAggregated\":0,\"udfType\":0},{\"aggregator\":\"in\",\"aggregatorName\":\"包含\",\"dataType\":\"str\",\"fieldAliasName\":\"ljc_group_x_table_name_0\",\"fieldDescription\":\"客位名称\",\"fieldFormula\":\"\",\"fieldGroup\":0,\"fieldId\":\"180731092950012620\",\"fieldName\":\"table_name\",\"fieldValue\":[\"鸡5号桌\",\"鸡3号桌\",\"鸡1号桌\"],\"granularity\":\"\",\"isBuildAggregated\":0,\"udfType\":0}],\"indexCondition\":[],\"kuduMaster\":\"hadoop207\",\"limit\":10,\"mongoConfig\":{\"mongoHost\":\"192.168.12.117\",\"mongoUserName\":\"lb\",\"mongoPort\":\"30017\",\"mongoPassword\":\"Lb#827\",\"mongoDb\":\"lb\"},\"page\":0,\"queryPoint\":1,\"queryType\":0,\"reportCode\":\"181218112123000236\",\"sessionGroup\":\"group_report\",\"sessionId\":\"3\",\"sortCondition\":[],\"sparkConfig\":{\"groupName\":\"group_report\",\"spark.default.parallelism\":\"20\",\"spark.sql.shuffle.partitions\":\"20\",\"spark.executor.instances\":\"3\",\"spark.executor.cores\":\"1\",\"spark.driver.cores\":\"1\",\"spark.driver.memory\":\"4000m\",\"spark.executor.memory\":\"4000m\",\"spark.scheduler.mode\":\"FAIR\",\"spark.custom.coalesce\":\"1\"},\"synSubmit\":true,\"tbId\":\"180620140417000008\",\"tbName\":\"dw_trade_bill_fact_p_group\",\"tracId\":\"1545104572000\"}"
    //    val param = "{\"compareCondition\":[],\"dataSourceType\":0,\"dbName\":\"cy7_2\",\"dimensionCondition\":[],\"enterTime\":1540371908060,\"filterCondition\":[{\"dataType\":\"str\",\"fieldAliasName\":\"ljc_filter_x_group_code_0\",\"fieldDescription\":\"集团\",\"fieldGroup\":0,\"fieldName\":\"group_code\",\"fieldValue\":[\"9759\"],\"isBuildAggregated\":0,\"udfType\":0}],\"indexCondition\":[{\"aggregator\":\"avg\",\"aggregatorName\":\"平均值-年环比\",\"aliasName\":\"用餐时长(平均值-年环比)\",\"dataType\":\"int\",\"fieldAliasName\":\"ljc_avg_x_dinner_time_0\",\"fieldDescription\":\"用餐时长\",\"fieldGroup\":0,\"fieldId\":\"180620140417000726\",\"fieldName\":\"dinner_time\",\"isBuildAggregated\":0,\"qoqConditionBean\":{\"fieldAliasName\":\"ljc_qoq_x_settle_biz_dateyear_0\",\"fieldDescription\":\"营业日\",\"fieldName\":\"settle_biz_date\",\"granularity\":\"year\",\"qoqRadixTime\":\"2018\",\"qoqReducedTime\":\"2017\",\"qoqResultType\":1},\"qoqType\":2,\"udfType\":0,\"uniqId\":\"1540366504000\"},{\"aggregator\":\"sum\",\"aggregatorName\":\"求和-月环比\",\"aliasName\":\"就餐人数(求和-月环比)\",\"dataType\":\"int\",\"fieldAliasName\":\"ljc_sum_x_people_qty_0\",\"fieldDescription\":\"就餐人数\",\"fieldGroup\":0,\"fieldId\":\"180620140417000705\",\"fieldName\":\"people_qty\",\"isBuildAggregated\":0,\"qoqConditionBean\":{\"fieldAliasName\":\"ljc_qoq_x_settle_biz_datemonth_0\",\"fieldDescription\":\"营业日\",\"fieldName\":\"settle_biz_date\",\"granularity\":\"month\",\"qoqRadixTime\":\"2017-10\",\"qoqReducedTime\":\"2017-09\",\"qoqResultType\":1},\"qoqType\":2,\"udfType\":0,\"uniqId\":\"1540370307000\"}],\"limit\":105,\"mongoConfig\":{\"mongoHost\":\"192.168.12.59\",\"mongoUserName\":\"lb_dev\",\"mongoPort\":\"27017\",\"mongoPassword\":\"lb_dev\",\"mongoDb\":\"lb\"},\"page\":0,\"queryPoint\":1,\"queryType\":0,\"reportCode\":\"181015091731004806\",\"sessionId\":\"2\",\"sortCondition\":[],\"sparkConfig\":{\"spark.executor.memory\":\"7273m\",\"spark.driver.memory\":\"7273m\",\"spark.driver.cores\":\"1\",\"spark.cassandra.connection.host\":\"192.168.12.203,192.168.12.204\",\"spark.custom.coalesce\":\"1\",\"spark.executor.cores\":\"1\",\"spark.sql.shuffle.partitions\":\"40\",\"spark.cassandra.connection.port\":\"9042\",\"spark.default.parallelism\":\"40\",\"spark.scheduler.mode\":\"FAIR\",\"spark.executor.instances\":\"3\"},\"tbId\":\"180620140417000008\",\"tbName\":\"dw_trade_bill_fact_p_group\",\"tracId\":\"1540371911000\"}"
    //    val param = "{\"compareCondition\":[],\"dataSourceType\":0,\"dbName\":\"cy7_2\",\"dimensionCondition\":[{\"aliasName\":\"时段\",\"dataType\":\"str\",\"fieldAliasName\":\"ljc_group_x_shiduan463963_0\",\"fieldDescription\":\"时段\",\"fieldFormula\":\"CASE WHEN ( ( date_format(table_open_time,'HH:mm:ss')  >= '08:00:00' and  date_format(table_open_time,'HH:mm:ss')  <= '09:00:00')  ) THEN '8-9点' WHEN ( ( date_format(table_open_time,'HH:mm:ss')  >= '09:00:00' and  date_format(table_open_time,'HH:mm:ss')  <= '10:00:00')  ) THEN '9-10点' WHEN ( ( date_format(table_open_time,'HH:mm:ss')  >= '10:00:00' and  date_format(table_open_time,'HH:mm:ss')  <= '11:00:00')  ) THEN '10-11点' WHEN ( ( date_format(table_open_time,'HH:mm:ss')  >= '11:00:00' and  date_format(table_open_time,'HH:mm:ss')  <= '12:00:00')  ) THEN '11-12点' WHEN ( ( date_format(table_open_time,'HH:mm:ss')  >= '12:00:00' and  date_format(table_open_time,'HH:mm:ss')  <= '13:00:00')  ) THEN '12-13点' WHEN ( ( date_format(table_open_time,'HH:mm:ss')  >= '13:00:00' and  date_format(table_open_time,'HH:mm:ss')  <= '14:00:00')  ) THEN '13-14点' WHEN ( ( date_format(table_open_time,'HH:mm:ss')  >= '14:00:00' and  date_format(table_open_time,'HH:mm:ss')  <= '15:00:00')  ) THEN '14-15点' WHEN ( ( date_format(table_open_time,'HH:mm:ss')  >= '15:00:00' and  date_format(table_open_time,'HH:mm:ss')  <= '16:00:00')  ) THEN '15-16点' WHEN ( ( date_format(table_open_time,'HH:mm:ss')  >= '16:00:00' and  date_format(table_open_time,'HH:mm:ss')  <= '17:00:00')  ) THEN '16-17点' WHEN ( ( date_format(table_open_time,'HH:mm:ss')  >= '17:00:00' and  date_format(table_open_time,'HH:mm:ss')  <= '18:00:00')  ) THEN '17-18点' WHEN ( ( date_format(table_open_time,'HH:mm:ss')  >= '18:00:00' and  date_format(table_open_time,'HH:mm:ss')  <= '19:00:00')  ) THEN '18-19点' WHEN ( ( date_format(table_open_time,'HH:mm:ss')  >= '19:00:00' and  date_format(table_open_time,'HH:mm:ss')  <= '20:00:00')  ) THEN '19-20点' WHEN ( ( date_format(table_open_time,'HH:mm:ss')  >= '20:00:00' and  date_format(table_open_time,'HH:mm:ss')  <= '21:00:00')  ) THEN '20-21点' WHEN ( ( date_format(table_open_time,'HH:mm:ss')  >= '21:00:00' and  date_format(table_open_time,'HH:mm:ss')  <= '22:00:00')  ) THEN '21-22点' WHEN ( ( date_format(table_open_time,'HH:mm:ss')  >= '22:00:00' and  date_format(table_open_time,'HH:mm:ss')  <= '23:59:59')  OR ( date_format(table_open_time,'HH:mm:ss')  >= '00:00:00' and  date_format(table_open_time,'HH:mm:ss')  <= '07:59:59')  ) THEN '其他'  ELSE '未分组' END\",\"fieldGroup\":0,\"fieldId\":\"181025100013034531\",\"fieldName\":\"shiduan463963\",\"isBuildAggregated\":1,\"udfType\":0,\"uniqId\":\"1540444249000\"}],\"enterTime\":1540445212342,\"filterCondition\":[{\"dataType\":\"str\",\"fieldAliasName\":\"ljc_filter_x_group_code_0\",\"fieldDescription\":\"集团\",\"fieldGroup\":0,\"fieldName\":\"group_code\",\"fieldValue\":[\"8391\"],\"isBuildAggregated\":0,\"udfType\":0}],\"indexCondition\":[{\"aggregator\":\"sum\",\"aggregatorName\":\"求和\",\"aliasName\":\"折前价格(求和)\",\"dataType\":\"double\",\"fieldAliasName\":\"ljc_sum_x_pre_discount_price_0\",\"fieldDescription\":\"折前价格\",\"fieldGroup\":0,\"fieldId\":\"180620140409000625\",\"fieldName\":\"pre_discount_price\",\"isBuildAggregated\":0,\"qoqType\":0,\"udfType\":0,\"uniqId\":\"1540444236000\"}],\"limit\":10,\"mongoConfig\":{\"mongoHost\":\"192.168.12.59\",\"mongoUserName\":\"lb_dev\",\"mongoPort\":\"27017\",\"mongoPassword\":\"lb_dev\",\"mongoDb\":\"lb\"},\"page\":0,\"queryPoint\":1,\"queryType\":0,\"reportCode\":\"181025131023005009\",\"sessionId\":\"0\",\"sortCondition\":[],\"sparkConfig\":{\"spark.executor.memory\":\"7273m\",\"spark.driver.memory\":\"7273m\",\"spark.driver.cores\":\"1\",\"spark.cassandra.connection.host\":\"192.168.12.203,192.168.12.204\",\"spark.custom.coalesce\":\"1\",\"spark.executor.cores\":\"1\",\"spark.sql.shuffle.partitions\":\"40\",\"spark.cassandra.connection.port\":\"9042\",\"spark.default.parallelism\":\"40\",\"spark.scheduler.mode\":\"FAIR\",\"spark.executor.instances\":\"3\"},\"tbId\":\"180620140409000007\",\"tbName\":\"dw_trade_bill_detail_fact_p_group\",\"tracId\":\"ljc_traceId_1717215857374228890\"}"
    //组装spark sql
    val sparkSqlCondition: SparkSqlCondition = new SparkSqlBuild().buildSqlStatement(param)
    val spark: SparkSession = SparkSourceContext.getSparkSession(sparkSqlCondition)
    println("")
    try {

      sqlExtLog.sessionId = sparkSqlCondition.getSessionId
      sqlExtLog.tracId = sparkSqlCondition.getTracId

      //结果集数量限制
      if (sparkSqlCondition.getLimit > 0) {
        defaultLimit = sparkSqlCondition.getLimit
      }

      val sqlStr: String = sparkSqlCondition.getSelectSql
      val sqlQoqStr: String = sparkSqlCondition.getSelectQoqSql
      logger.info(s"【SQLExtInterpreter-日志】-【execute】-主体SQL：$sqlStr")
      logger.info(s"【SQLExtInterpreter-日志】-【execute】-同环比SQL：$sqlQoqStr")

      //3-主体SQL执行
      val sqlExecuteBeginTime = System.currentTimeMillis()
      var df: DataFrame = null
      if (sqlStr.nonEmpty) {
        df = spark.sql(sqlStr)
      }
      sqlExtLog.sqlExecuteElapsedTime = System.currentTimeMillis() - sqlExecuteBeginTime

      //4-同环比
      val qoqBeginTime = System.currentTimeMillis()
      df = handleQoqSql(df, sparkSqlCondition, spark)
      sqlExtLog.qoqElapsedTime = System.currentTimeMillis() - qoqBeginTime
      import DataFrameExtensions._

      if (df.nonEmpty()) {

        df.persist()
        //5-自定义字段作为筛选项处理
        df = handleCustomField(df, sparkSqlCondition)

        //6-筛选项的值处理
        df = handleFilterValueDf(df, sparkSqlCondition)

        //7-对比项反转
        val comparePivotBeginTime = System.currentTimeMillis()
        df = handleComparePivot(df, sparkSqlCondition)
        sqlExtLog.comparePivotElapsedTime = System.currentTimeMillis() - comparePivotBeginTime

        //8-结果集排序与截取
        df = handleSortAndLimitResult(df, sparkSqlCondition)

        //9-分页处理
        val total = df.count()
        df = pagination(df, sparkSqlCondition, total)
        //        df.show(1000)
        // 10-获取数据结构
        val jsonString = df.schema.json
        val jSchema = parse(jsonString)

        // 11-获取数据
        val rows = df.take(defaultLimit).map(_.toSeq)
        val jRows = Extraction.decompose(rows)
        val endTime = System.currentTimeMillis()

        df.unpersist()
        //        spark.catalog.clearCache()
        println("总耗时：：：：：：" + (endTime - beginTime))
      }
      df.show()
    } catch {
      case e: InvocationTargetException =>
        logger.info(s"Fail to execute query $param", e.getTargetException)
        val cause = e.getTargetException
        logger.info(s"Error ${cause.getMessage},${cause.getStackTrace.map(_.toString)}")
      case NonFatal(f) =>
        logger.info(s"Fail to execute query $param", f)
        logger.info(s"Error ${f.getMessage},${f.getStackTrace.map(_.toString)}")
    }
    finally {
      //      val endTime = System.currentTimeMillis()
      //      sqlExtLog.endTime = DateUtils.convertTimeToString(endTime, DateUtils.MILLS_SECOND_OF_DATE_FRM)
      //      sqlExtLog.totalElapsedTime = endTime - beginTime
      //      saveSqlExtLog(sqlExtLog, sparkSqlCondition)
    }
  }


  /**
    * 同环比指标计算
    *
    * @param df                主体数据集（非同比计算数据集）
    * @param sparkSqlCondition 同环比计算条件
    * @param sparkSession      hive上下文
    */
  private[this] def handleQoqSql(df: DataFrame, sparkSqlCondition: SparkSqlCondition, sparkSession: SparkSession): DataFrame = {
    var dfQoqResult = df
    //排序项
    val orderMap: util.Map[String, String] = sparkSqlCondition.getOrderByMap
    //字段排序
    val orderList: List[Column] = sparkOrderCols(orderMap)
    //交叉表排序项
    val crosstabMap: util.Map[String, String] = sparkSqlCondition.getCrosstabByMap

    //同环比
    if (sparkSqlCondition.getQoqList != null && sparkSqlCondition.getQoqList.size() > 0) {
      //同环比SQL
      var df1: DataFrame = sparkSession.sql(sparkSqlCondition.getSelectQoqSql)
      //同环比计算
      dfQoqResult = handleQoqDf(df, df1, sparkSqlCondition)
    }
    if (orderList.nonEmpty && crosstabMap.isEmpty) {
      dfQoqResult = dfQoqResult.na.fill(fillValue).orderBy(orderList: _*)
    }
    dfQoqResult
  }

  /**
    * 同环比计算
    *
    * @param df0               主体SQL数据集
    * @param dfQoq             同环比SQL数据集
    * @param sparkSqlCondition 同环比计算条件
    */
  private[this] def handleQoqDf(df0: DataFrame, dfQoq: DataFrame, sparkSqlCondition: SparkSqlCondition): DataFrame = {
    var df1 = df0
    //同环比计算条件
    val qoqList: List[QoqDTO] = JavaConversions.asScalaBuffer(sparkSqlCondition.getQoqList).toList

    if (qoqList.nonEmpty) {
      //查询项字段
      var selectList: List[String] = if (sparkSqlCondition.getSelectList != null) JavaConversions.asScalaBuffer(sparkSqlCondition.getSelectList).toList else Nil
      //分组字段
      var groupList: List[String] = if (sparkSqlCondition.getGroupList != null) JavaConversions.asScalaBuffer(sparkSqlCondition.getGroupList).toList else Nil
      //对比字段
      val compareList: List[String] = if (sparkSqlCondition.getCompareList != null) JavaConversions.asScalaBuffer(sparkSqlCondition.getCompareList).toList else Nil
      //聚合字段
      val sparkAggMap: mutable.Map[String, util.List[String]] = sparkSqlCondition.getSparkAggMap.asScala
      //聚合字段
      val aggList: List[Column] = sparkAgg(sparkAggMap)
      if (compareList != null) {
        groupList = compareList.:::(groupList)
      }
      //数据集连接类型
      var joinType = joinTypeInner
      if (groupList.isEmpty) {
        joinType = joinTypeCross
      }
      //同环比计算
      var df2 = handleQoq(dfQoq, qoqList.head, groupList, aggList)

      //同环比结果集合并
      if (joinType == joinTypeCross) {
        qoqList.tail.foreach(qoq => df2 = df2.crossJoin(handleQoq(dfQoq, qoq, groupList, aggList)))
      }
      if (joinType == joinTypeInner) {
        qoqList.tail.foreach(qoq => df2 = df2.join(handleQoq(dfQoq, qoq, groupList, aggList), groupList, joinTypeInner))
      }

      //主体结果集与同环比结果合并
      if (df0 == null) {
        df1 = df2
      } else if (joinType == joinTypeCross) {
        df1 = df0.crossJoin(df2)
      } else if (joinType == joinTypeInner) {
        df1 = df0.join(df2, groupList, joinType)
      }
      var selectCols: List[Column] = List()
      selectList = groupList.:::(selectList)
      selectList.distinct.foreach(select => selectCols = selectCols :+ col(select))

      //重新查询,统计和排序,确保顺序与参数一致
      //分组字段不为空
      if (groupList.nonEmpty) {
        df1 = df1.select(selectCols: _*).groupBy(groupList.head, groupList.tail: _*).agg(aggList.head, aggList.tail: _*)
      }
      else {
        df1 = df1.select(selectCols: _*).agg(aggList.head, aggList.tail: _*)
      }
    }
    df1
  }

  /** 同环比计算
    * 1）环比增长率=（本期数－上期数）/上期数×100%
    * 2）同比增长率=（本期数－同期数）/同期数×100%
    *
    * @param dfQoq     数据集
    * @param qoqDTO    同环比条件
    * @param groupList 分组字段
    */
  private[this] def handleQoq(dfQoq: DataFrame, qoqDTO: QoqDTO, groupList: List[String], sparkAggList: List[Column]): DataFrame = {
    //根据同环比字段反转
    var df1 = dfQoq
    if (groupList.nonEmpty) {
      df1 = dfQoq.coalesce(defaultNumPartitions).groupBy(groupList.head, groupList.tail: _*)
        .pivot(qoqDTO.getQoqTimeAliasName, Seq(qoqDTO.getQoqRadixTime, qoqDTO.getQoqReducedTime))
        .agg(sum(qoqDTO.getQoqIndexAliasName))
    } else {
      df1 = dfQoq.coalesce(defaultNumPartitions).groupBy()
        .pivot(qoqDTO.getQoqTimeAliasName, Seq(qoqDTO.getQoqRadixTime, qoqDTO.getQoqReducedTime))
        .agg(sum(qoqDTO.getQoqIndexAliasName))
    }
    //组装查询项
    var selectCols: List[Column] = List()
    groupList.foreach(group => selectCols = selectCols :+ col(group))

    //同环比基数时间
    val qoqRadixTime = qoqDTO.getQoqRadixTime
    //同环比对比时间
    val qoqReducedTime = qoqDTO.getQoqReducedTime
    //同环比指标别名
    val qoqIndexAliasName = qoqDTO.getQoqIndexAliasName
    //同环比基数时间列
    val qoqRadixTimeCol: Column = when(isnull(col(qoqRadixTime)), 0).otherwise(col(qoqRadixTime))
    //同环比对比时间列
    val qoqReducedTimeCol: Column = when(isnull(col(qoqReducedTime)), 0).otherwise(col(qoqReducedTime))
    //同环比对比时间列,作为分母

    //增长值表达式
    val growthValueExpression: Column = (qoqRadixTimeCol - qoqReducedTimeCol).as(qoqIndexAliasName)

    //增长率表达式
    val growthRateExpression: Column = ((qoqRadixTimeCol - qoqReducedTimeCol) / qoqReducedTimeCol).as(qoqIndexAliasName)

    //增长值
    if (qoqDTO.getQoqResultType == 1) {
      selectCols = selectCols :+ growthValueExpression
    }
    //增长率
    if (qoqDTO.getQoqResultType == 2) {
      selectCols = selectCols :+ growthRateExpression
    }
    df1 = df1.select(selectCols: _*)
    df1
  }

  //取前后n条数据排序
  private[this] def handleSortAndLimitResult(limitDf: DataFrame, sparkSqlCondition: SparkSqlCondition): DataFrame = {
    var df = limitDf
    //对比条件
    val compareList: util.List[String] = sparkSqlCondition.getCompareList
    //交叉表排序条件
    val crosstabOrderMap: util.Map[String, String] = sparkSqlCondition.getCrosstabByMap

    //交叉表只有维度和指标
    if (Objects.isNull(compareList) || compareList.isEmpty) {
      if (Objects.nonNull(crosstabOrderMap) && !crosstabOrderMap.isEmpty) {
        val orderCols = sparkOrderCols(crosstabOrderMap)
        df = df.na.fill(fillValue).orderBy(orderCols: _*)
      }
    }
    df
  }

  //筛选项的值处理
  private[this] def handleFilterValueDf(sqlDf: DataFrame, sparkSqlCondition: SparkSqlCondition): DataFrame = {
    var df = sqlDf
    //指标条件
    val indexList: util.List[String] = sparkSqlCondition.getIndexList

    //筛选项的值查询,对结果集去重
    if (sparkSqlCondition.getQueryType == 1) {
      if (indexList != null) {
        sparkSqlCondition.setLimit(Integer.MAX_VALUE)
        val dataType: DataType = df.select(indexList.get(0)).schema.head.dataType
        if (dataType == StringType) {
          df = df.distinct().where(indexList.get(0).concat("!=''"))
        }
        else {
          df = df.distinct()
        }
      }
    }
    df
  }

  //排序条件转换
  private[this] def sparkOrderCols(orderMap: util.Map[String, String]): List[Column] = {
    var orderColumnList: List[Column] = Nil
    val orderColsMap = orderMap.asScala
    if (orderColsMap != null && orderColsMap.nonEmpty) {
      for (key <- orderColsMap.keys) {
        val value: String = orderColsMap(key)
        if (value.toUpperCase() == Constant.SORT_ASC) {
          orderColumnList = orderColumnList :+ asc(key)
        }
        if (value.toUpperCase() == Constant.SORT_DESC) {
          orderColumnList = orderColumnList :+ desc(key)
        }
      }
    }
    orderColumnList
  }


  /**
    * 结果集分页
    *
    * @param df                待分页数据集
    * @param sparkSqlCondition 分页条件
    * @param totalNum          结果集总数
    * @return 返回分页后的数据集
    */
  private[this] def pagination(df: DataFrame, sparkSqlCondition: SparkSqlCondition, totalNum: Long): DataFrame = {
    //分页后结果集
    var dfResult = df

    //当前页
    val page = sparkSqlCondition.getPage

    //结果数量
    val limit: Int = sparkSqlCondition.getLimit

    //跳过之前页的数据
    var skipNum = 0

    //结果集实际总数量大于请求的数量
    if (totalNum > limit) {
      if (page > 0) {
        skipNum = (page - 1) * limit
      }
      //取后n条，跳过（总条数-limit），截取 limit 条数据
      if (sparkSqlCondition.getQueryPoint == 2) {
        skipNum = totalNum.toInt - limit
      }
      if (skipNum > 0) {
        val df1 = df.limit(skipNum)
        dfResult = df.except(df1).limit(limit)
      }
      else {
        dfResult = df.limit(limit)
      }
    }
    else {
      dfResult = df.limit(limit)
    }
    dfResult
  }

  //自定义字段作为筛选项处理
  private[this] def handleCustomField(sqlDf: DataFrame, sparkSqlCondition: SparkSqlCondition): DataFrame = {
    var df = sqlDf
    //自定义字段筛选项
    val customField: util.List[String] = sparkSqlCondition.getFilterCustomFieldList
    //筛选表达式
    val filterFormula: String = sparkSqlCondition.getFilterFormula
    //删除筛选列标识
    val delFilterField: Boolean = sparkSqlCondition.getDelFilterField

    //结果集筛选
    if (filterFormula != null && !filterFormula.isEmpty) {
      df = df.where(filterFormula)
    }
    //结果集删除筛选列
    if (delFilterField && customField != null && !customField.isEmpty) {
      df = df.drop(customField.asScala: _*)
    }
    df
  }

  //对比项反转处理
  private[this] def handleComparePivot(df: DataFrame, sparkSqlCondition: SparkSqlCondition): DataFrame = {
    //对比项
    val compareList: List[String] = if (sparkSqlCondition.getCompareList != null) JavaConversions.asScalaBuffer(sparkSqlCondition.getCompareList).toList else Nil
    //对比项为空，直接退出此方法
    if (compareList == null || compareList.isEmpty) {
      return df
    }
    var df1 = df

    //分组项
    val groupList: List[String] = if (sparkSqlCondition.getGroupList != null) JavaConversions.asScalaBuffer(sparkSqlCondition.getGroupList).toList else Nil
    //对比项
    val sparkAggMap: mutable.Map[String, util.List[String]] = sparkSqlCondition.getSparkAggMap.asScala
    //交叉表排序项
    val crosstabMap: util.Map[String, String] = sparkSqlCondition.getCrosstabByMap
    //对比列别名
    val pivotsAlias: String = PIVOT_ALIAS
    //对比列的值
    var compareValues: List[String] = List()

    //多个对比列合并并起别名
    df1 = combineCompareColumn(df1, sparkSqlCondition)

    //没有指定排序项时，反转列默认升序排序
    if (sparkSqlCondition.getCompareSortFlag || !sparkSqlCondition.getDimensionIsEmpty) {
      compareValues = df1.coalesce(defaultNumPartitions).select(pivotsAlias).orderBy(asc(pivotsAlias)).distinct().limit(compareLimit).rdd.map(r => r.get(0).toString).collect().toList
    }
    else {
      compareValues = df1.coalesce(defaultNumPartitions).select(pivotsAlias).distinct().limit(compareLimit).rdd.map(r => r.get(0).toString).collect().toList
    }
    logger.info(s"【SQLExtInterpreter-日志】-【sparkSqlPivot】-对比列的值：${objectToJson(compareValues)}")

    val compareValuesSize = compareValues.size
    //对比项取后n条
    if (sparkSqlCondition.getQueryPoint == 2) {
      val totalNum = df1.count()
      val skipNum = totalNum.toInt - sparkSqlCondition.getLimit
      if (skipNum < compareValuesSize) {
        compareValues = compareValues.slice(skipNum, compareValuesSize)
      } else {
        compareValues = compareValues.slice(0, sparkSqlCondition.getLimit)
      }
    } else {
      compareValues = compareValues.slice(0, sparkSqlCondition.getLimit)
    }

    //聚合字段
    val aggList: List[Column] = sparkAgg(sparkAggMap)
    if (aggList != null && aggList.nonEmpty) {
      var pivotFlag = false
      //对比项反转
      if (groupList != null && groupList.nonEmpty) {
        df1 = df1.groupBy(groupList.head, groupList.tail: _*).pivot(pivotsAlias, compareValues).agg(aggList.head, aggList.tail: _*)
        pivotFlag = true
      }
      else {
        df1 = df1.groupBy().pivot(pivotsAlias, compareValues).agg(aggList.head, aggList.tail: _*)
        pivotFlag = true
      }
      //交叉表排序
      if (!crosstabMap.isEmpty && pivotFlag) {
        val orderList: List[Column] = sparkOrderCols(crosstabMap)
        df1 = df1.orderBy(orderList: _*)
      }
    }
    df1
  }

  //聚合条件转换
  private[this] def sparkAgg(aggMap: mutable.Map[String, util.List[String]]): List[Column] = {
    var aggColumnList: List[Column] = Nil
    if (aggMap != null && aggMap.nonEmpty) {
      logger.info(s"【SQLExtInterpreter-日志】-【sparkSqlAgg】-aggColumnList结果: $aggMap")
      for (key <- aggMap.keys) {
        val aggs: mutable.Buffer[String] = aggMap(key).asScala
        for (agg <- aggs) {
          aggColumnList = aggColumnList :+ min(agg).alias(agg)
        }
      }
      logger.info(s"【SQLExtInterpreter-日志】-【sparkSqlAgg】-aggColumnList结果: $aggColumnList")
    }
    aggColumnList
  }

  //合并对比列,并设置别名为【y】
  private[this] def combineCompareColumn(df: DataFrame, sparkSqlCondition: SparkSqlCondition): DataFrame = {
    import org.apache.spark.sql.Column
    import org.apache.spark.sql.functions.{col, concat_ws}
    var df1 = df
    //对比项
    val compareList: util.List[String] = sparkSqlCondition.getCompareList
    //查询项
    val selectList: util.List[String] = sparkSqlCondition.getSelectList

    logger.info(s"【SQLExtInterpreter-日志】-【combineCompareColumn】-对比列：${objectToJson(compareList)}")
    logger.info(s"【SQLExtInterpreter-日志】-【combineCompareColumn】-查询项：${objectToJson(selectList)}")

    //没有对比项直接跳过
    if (compareList == null || compareList.isEmpty) {
      return df1
    }
    //spark sql api 格式的查询项
    var colList: List[Column] = Nil
    //对比列
    var compareCols: List[Column] = Nil

    //查询项中移除对比项
    selectList.removeAll(compareList)
    for (select <- selectList.asScala) {
      colList = colList :+ col(select)
    }
    for (compare <- compareList.asScala) {
      compareCols = compareCols :+ when(col(compare).isNull, unknown).otherwise(col(compare))
    }
    //多个对比项
    if (compareList.size() > 1) {
      colList = colList :+ concat_ws(compareSplitChar, compareCols: _*).as(PIVOT_ALIAS)
    } else {
      colList = colList :+ compareCols.head.as(PIVOT_ALIAS)
    }
    df1 = df1.select(colList: _*)
    df1
  }

  //转json字符串
  private[this] def objectToJson(obj: Any): String = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val out = new StringWriter
    mapper.writeValue(out, obj)
    val json = out.toString
    json
  }

  //配置spark
  private[this] def sparkConfig(sparkSession: SparkSession, sparkSqlCondition: SparkSqlCondition): SparkSession = {
    var sparkConfMap: mutable.Map[String, String] = sparkSqlCondition.getSparkConfig.asScala
    logger.info(s"【SQLExtInterpreter-日志】-【sparkConfig】-spark配置项：$sparkConfMap")
    if (sparkConfMap != null && sparkConfMap.nonEmpty) {
      val coalesce = "spark.custom.coalesce"
      if (sparkConfMap.contains(coalesce)) {
        defaultNumPartitions = Integer.valueOf(sparkConfMap(coalesce))
        sparkConfMap = sparkConfMap.-(coalesce)
      }
      sparkConfMap.keys.foreach(key => sparkSession.conf.set(key, sparkConfMap(key)))
    }

    //连接kudu会话初始化
    val dataSourceType = sparkSqlCondition.getDataSourceType
    if (dataSourceType == 1) {
      //kudu 数据表全名称
      val kuduTable = sparkSqlCondition.getKeyspace.concat(".").concat(sparkSqlCondition.getTable)

      val df: DataFrame = sparkSession
        .read
        .options(Map(
          "kudu.master" -> sparkSqlCondition.getKuduMaster,
          "kudu.table" -> kuduTable
        ))
        .kudu
        .filter(sparkSqlCondition.getCassandraFilter)
      val tempView = "temp_kudu_table_".concat(System.currentTimeMillis().toString)

      df.createOrReplaceTempView(tempView)
      val mainSql = sparkSqlCondition.getSelectSql
      val qoqSql = sparkSqlCondition.getSelectQoqSql

      sparkSqlCondition.setSelectSql(mainSql.replace(kuduTable, tempView))
      sparkSqlCondition.setSelectQoqSql(qoqSql.replace(kuduTable, tempView))
    }
    sparkSession
  }


  /**
    * 保存日志到mongodb
    *
    * @param log 日志对象
    **/
  def saveSqlExtLog(log: SqlExtInterpreterLog_v3, sparkSqlCondition: SparkSqlCondition): Unit = {
    try {
      val mongoMap = sparkSqlCondition.getMongoConfigMap
      val mongoClient: MongoClient = getMongoClient(mongoMap.get("mongoHost"),
        mongoMap.get("mongoPort").toInt,
        mongoMap.get("mongoDb"),
        mongoMap.get("mongoUserName"),
        mongoMap.get("mongoPassword"))

      val collection: DBCollection = mongoClient.getDB(mongoMap.get("mongoDb")).getCollection("loongboss_livy_server_log")
      import com.google.gson.Gson
      val gson = new Gson
      val dbObject: DBObject = JSON.parse(gson.toJson(log)).asInstanceOf[DBObject]
      collection.insert(dbObject)
    } catch {
      case exception: Exception =>
        logger.error("【SQLExtInterpreter::saveSqlExtLog】-MongoDB记录日志出现异常")
    }
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
  def getMongoClient(ip: String, port: Int, dbName: String, loginName: String, password: String): MongoClient = {
    val server = new ServerAddress(ip, port)
    //注意：MongoCredential中有6种创建连接方式，这里使用MONGODB_CR机制进行连接。如果选择错误则会发生权限验证失败
    val credentials = MongoCredential.createScramSha1Credential(loginName, dbName, password.toCharArray)
    val mongoClient = MongoClient(server, List(credentials))
    mongoClient
  }

}


//object DataFrameExtensions {
//  implicit def extendedDataFrame(dataFrame: DataFrame): ExtendedDataFrame =
//    new ExtendedDataFrame(dataFrame: DataFrame)
//
//  class ExtendedDataFrame(dataFrame: DataFrame) {
//    def isEmpty: Boolean = {
//      Try {
//        dataFrame.first.length != 0
//      } match {
//        case Success(_) => false
//        case Failure(_) => true
//      }
//    }
//
//    def nonEmpty(): Boolean = !isEmpty
//  }
//
//}
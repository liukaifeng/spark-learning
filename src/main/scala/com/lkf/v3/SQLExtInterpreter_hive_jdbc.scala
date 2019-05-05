package com.lkf.v3

import java.lang.reflect.InvocationTargetException
import java.util
import java.util.Objects

import com.mongodb.casbah.{MongoClient, MongoCredential}
import com.mongodb.util.JSON
import com.mongodb.{DBCollection, DBObject, ServerAddress}
import org.apache.livy.client.ext.model.Constant.PIVOT_ALIAS
import org.apache.livy.client.ext.model.{DateUtils, SparkSqlCondition, _}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StringType, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.{JavaConversions, mutable}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object SQLExtInterpreter_hive_jdbc {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private implicit def formats = DefaultFormats

  //对比项分隔符
  private val compareSplitChar = Constant.COMPARE_SPLIT_CHAR

  //逗号分隔符
  private val splitDot = ","


  private val fillValue = -123

  //对比列值数量限制
  private val compareLimit = 100

  //默认分区数量
  private var defaultNumPartitions = 10

  private val unknown = "未知"

  private val joinTypeInner = "left"
  private val joinTypeCross = "cross"

  def main(args: Array[String]): Unit = {
    val sqlExtLog: SqlExtInterpreterLog_v3 = new SqlExtInterpreterLog_v3
    val beginTime: Long = System.currentTimeMillis()
    sqlExtLog.beginTime = DateUtils.convertTimeToString(beginTime, DateUtils.MILLS_SECOND_OF_DATE_FRM)
    System.setProperty("hadoop.home.dir", "D:\\Java\\hadoop-3.0")
    val param = "{\"accessToken\":\"6a6da873-c91b-4f26-bf38-0f4b290bda5b\",\"compareCondition\":[{\"aliasName\":\"性别\",\"dataType\":\"str\",\"fieldAliasName\":\"ljc_compare_x_di4liestr1556433491000_0\",\"fieldDescription\":\"性别\",\"fieldGroup\":0,\"fieldId\":\"190428113051002416\",\"fieldName\":\"di4lie\",\"granularity\":\"str\",\"isBuildAggregated\":0,\"originDataType\":\"str\",\"udfType\":0,\"uniqId\":\"1556433491000\"}],\"computeKind\":\"sql_ext\",\"dataSourceType\":1,\"dbName\":\"impala::e000112\",\"dimensionCondition\":[{\"aliasName\":\"部门\",\"dataType\":\"str\",\"fieldAliasName\":\"ljc_group_x_di3liestr1556433489000_0\",\"fieldDescription\":\"部门\",\"fieldGroup\":0,\"fieldId\":\"190428113051002415\",\"fieldName\":\"di3lie\",\"granularity\":\"str\",\"isBuildAggregated\":0,\"originDataType\":\"str\",\"udfType\":0,\"uniqId\":\"1556433489000\"}],\"filterCondition\":[],\"hiveJdbcConfig\":{\"hiveUrl\":\"jdbc:hive2://192.168.12.204:21050/%s;auth=noSasl\",\"hiveUser\":\"\",\"hivePassword\":\"\"},\"indexCondition\":[{\"aggregator\":\"dis_count\",\"aggregatorName\":\"去重计数-百分比\",\"aliasName\":\"工作天数(去重计数-百分比)\",\"dataType\":\"double\",\"fieldAliasName\":\"ljc_dis_count_x_di9liedouble1556433500000_0\",\"fieldDescription\":\"工作天数\",\"fieldGroup\":0,\"fieldId\":\"190428113051002421\",\"fieldName\":\"di9lie\",\"granularity\":\"double\",\"isBuildAggregated\":0,\"originDataType\":\"double\",\"qoqType\":7,\"udfType\":0,\"uniqId\":\"1556433500000\"}],\"indexDoubleCondition\":[],\"kuduMaster\":\"hadoop207\",\"limit\":10,\"maxWaitSeconds\":60,\"mongoConfig\":{\"mongoHost\":\"192.168.12.117\",\"mongoUserName\":\"lb\",\"mongoPort\":\"30017\",\"mongoPassword\":\"Lb#827\",\"mongoDb\":\"lb\"},\"page\":0,\"platformVersion\":\"0\",\"queryPoint\":1,\"queryType\":0,\"reportCode\":\"190428111417000046\",\"sessionGroup\":\"group_report\",\"sessionId\":\"5\",\"sortCondition\":[],\"sparkConfig\":{\"groupName\":\"group_report\",\"spark.default.parallelism\":\"20\",\"spark.sql.shuffle.partitions\":\"20\",\"spark.executor.instances\":\"2\",\"spark.executor.cores\":\"2\",\"spark.driver.cores\":\"1\",\"spark.driver.memory\":\"2000m\",\"spark.executor.memory\":\"2000m\",\"spark.scheduler.mode\":\"FAIR\",\"spark.custom.coalesce\":\"1\"},\"synSubmit\":true,\"tbId\":\"190428113051000136\",\"tbName\":\"tonghuanbi_4281_qsl_000112\",\"tracId\":\"15570215330006136640871\"}"
    //    val param = "{\"accessToken\":\"34763405-afd4-44bd-bd86-fa277dc2879b\",\"compareCondition\":[{\"aliasName\":\"营业日\",\"dataType\":\"datetime\",\"fieldAliasName\":\"ljc_compare_x_settle_biz_date1553235288000_0\",\"fieldDescription\":\"营业日\",\"fieldGroup\":0,\"fieldId\":\"180620140417000718\",\"fieldName\":\"settle_biz_date\",\"isBuildAggregated\":0,\"originDataType\":\"datetime\",\"udfType\":0,\"uniqId\":\"1553235288000\"}],\"computeKind\":\"sql_ext\",\"dataSourceType\":1,\"dbName\":\"impala::e000\",\"dimensionCondition\":[{\"aliasName\":\"门店名称\",\"dataType\":\"str\",\"fieldAliasName\":\"ljc_group_x_store_name1553235279000_0\",\"fieldDescription\":\"门店名称\",\"fieldGroup\":0,\"fieldId\":\"180620140418000824\",\"fieldName\":\"store_name\",\"isBuildAggregated\":0,\"originDataType\":\"str\",\"udfType\":0,\"uniqId\":\"1553235279000\"}],\"filterCondition\":[{\"dataType\":\"int\",\"fieldAliasName\":\"ljc_filter_x_group_codefilter_0\",\"fieldDescription\":\"集团\",\"fieldGroup\":0,\"fieldName\":\"group_code\",\"fieldValue\":[\"9759\"],\"isBuildAggregated\":0,\"originDataType\":\"int\",\"udfType\":0}],\"hiveJdbcConfig\":{\"hiveUrl\":\"jdbc:hive2://192.168.12.204:21050/%s;auth=noSasl\",\"hiveUser\":\"\",\"hivePassword\":\"\"},\"indexCondition\":[{\"aggregator\":\"sum\",\"aggregatorName\":\"求和\",\"aliasName\":\"实际收入（实收）(求和)\",\"dataType\":\"double\",\"fieldAliasName\":\"ljc_sum_x_real_income1553235341000_0\",\"fieldDescription\":\"实际收入（实收）\",\"fieldGroup\":0,\"fieldId\":\"180620140418000779\",\"fieldName\":\"real_income\",\"isBuildAggregated\":0,\"originDataType\":\"double\",\"qoqType\":0,\"udfType\":0,\"uniqId\":\"1553235341000\"}],\"indexDoubleCondition\":[],\"kuduMaster\":\"hadoop207\",\"limit\":1500,\"maxWaitSeconds\":60,\"mongoConfig\":{\"mongoHost\":\"192.168.12.117\",\"mongoUserName\":\"lb\",\"mongoPort\":\"30017\",\"mongoPassword\":\"Lb#827\",\"mongoDb\":\"lb\"},\"page\":0,\"platformVersion\":\"0\",\"queryPoint\":0,\"queryType\":0,\"reportCode\":\"181115134910005556\",\"sessionGroup\":\"group_report\",\"sessionId\":\"0\",\"sortCondition\":[{\"dataType\":\"\",\"fieldAliasName\":\"实际收入（实收）(求和):%2017-09-01\",\"fieldDescription\":\"\",\"fieldGroup\":0,\"fieldId\":\"实际收入（实收）(求和):%2017-09-01\",\"fieldName\":\"实际收入（实收）(求和):%2017-09-01\",\"isBuildAggregated\":0,\"sortFlag\":\"desc\",\"sortType\":1,\"udfType\":0}],\"sparkConfig\":{\"groupName\":\"group_report\",\"spark.default.parallelism\":\"20\",\"spark.sql.shuffle.partitions\":\"20\",\"spark.executor.instances\":\"2\",\"spark.executor.cores\":\"2\",\"spark.driver.cores\":\"1\",\"spark.driver.memory\":\"2000m\",\"spark.executor.memory\":\"2000m\",\"spark.scheduler.mode\":\"FAIR\",\"spark.custom.coalesce\":\"1\"},\"synSubmit\":true,\"tbId\":\"180620140417000008\",\"tbName\":\"dw_trade_bill_fact_p_group_n2\",\"tracId\":\"1553235392000\"}"

    //组装spark sql
    val sparkSqlCondition: SparkSqlCondition = new SparkSqlBuild().buildSqlStatement(param)
    val sparkSession: SparkSession = SparkSourceContext.getSparkSession(sparkSqlCondition)

    try {
      sqlExtLog.sessionId = sparkSqlCondition.getSessionId
      sqlExtLog.tracId = sparkSqlCondition.getTracId
      sparkSqlCondition.getLimit
      var sqlStr: String = sparkSqlCondition.getSelectSql
      logger.info(s"【SQLExtInterpreter-日志】-【execute】-主体SQL：$sqlStr")

      //3-主体SQL执行
      val sqlExecuteBeginTime = System.currentTimeMillis()
      var df: DataFrame = null

      if (sqlStr.nonEmpty) {
        sqlExtLog.mainSql = sqlStr
        if (sparkSqlCondition.getSecondaryFlag) {
          df = HiveJdbcUtil.execute2DataFrame(sparkSession, sparkSqlCondition, sqlStr)
          df.show()
        }
        else {
          val map = HiveJdbcUtil.execute2Result(sparkSqlCondition, sqlStr)
          val schema = map("schema").asInstanceOf[StructType]
          val rows = map("rowList").asInstanceOf[List[Row]]
          val jSchema = parse(schema.json)
          val jRows = Extraction.decompose(rows.toArray.map(_.toSeq))
          println(jSchema)
          println(jRows)
          return
        }
      }

      sqlExtLog.sqlExecuteElapsedTime = System.currentTimeMillis() - sqlExecuteBeginTime

      //交叉表排序条件非空
      val crossOrderByNonNull = Objects.nonNull(sparkSqlCondition.getCrosstabByMap) && !sparkSqlCondition.getCrosstabByMap.isEmpty
      //无对比项
      val compareIsNull = Objects.isNull(sparkSqlCondition.getCompareList) || sparkSqlCondition.getCompareList.isEmpty

      //交叉表只有维度和指标，排序及取前后n条处理
      if (crossOrderByNonNull && compareIsNull) {
        df = handleSortAndLimitResult(df, sparkSqlCondition)
      }

      //5-自定义字段作为筛选项处理
      df = handleCustomField(df, sparkSqlCondition)

      //6-筛选项的值处理
      df = handleFilterValueDf(df, sparkSqlCondition)

      //7-对比项反转
      val comparePivotBeginTime = System.currentTimeMillis()
      if (!compareIsNull) {
        df = handleComparePivot(df.coalesce(defaultNumPartitions), sparkSqlCondition)
      }
      sqlExtLog.comparePivotElapsedTime = System.currentTimeMillis() - comparePivotBeginTime

      // 10-获取数据结构
      val jsonString = df.schema.json
      val jSchema = parse(jsonString)
      // 11-获取数据
      val rows = df.take(sparkSqlCondition.getLimit).map(_.toSeq)
      val jRows = Extraction.decompose(rows)
      df.show()
      df.unpersist()
      println(jSchema)
      println(jRows)
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
    * 交叉表只有维度和指标情况下排序及取前后n条处理
    *
    * @param limitDf           待处理数据集
    * @param sparkSqlCondition 请求条件
    **/
  private[this] def handleSortAndLimitResult(limitDf: DataFrame, sparkSqlCondition: SparkSqlCondition): DataFrame = {
    var df = limitDf
    //交叉表排序条件
    val orderCols = sparkOrderCols(sparkSqlCondition.getCrosstabByMap)

    //交叉表排序
    //    df = df.na.fill(fillValue).orderBy(orderCols: _*)
    df = df.orderBy(orderCols: _*)

    //取后n条数据
    if (sparkSqlCondition.getQueryPoint == 2) {
      //总条数
      val totalNum = df.count()
      //总条数大于请求条数再进行截取
      if (totalNum.toInt > sparkSqlCondition.getLimit) {
        //去掉的条数
        val skipNum = totalNum.toInt - sparkSqlCondition.getLimit
        //去掉的数据
        val df1 = df.limit(skipNum)
        //取指定条数数据
        df = df.except(df1).limit(sparkSqlCondition.getLimit)
      }
    }
    df
  }

  //筛选项的值处理
  private[this] def handleFilterValueDf(sqlDf: DataFrame, sparkSqlCondition: SparkSqlCondition): DataFrame = {
    var df = sqlDf
    //指标条件
    val indexList: util.List[String] = sparkSqlCondition.getIndexList
    //筛选项的值查询,对结果集空值过滤
    if (sparkSqlCondition.getQueryType == 1) {
      if (indexList != null) {
        val dataType: DataType = df.select(indexList.get(0)).schema.head.dataType
        if (dataType == StringType) {
          df = df.where(indexList.get(0).concat("!=''"))
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
          orderColumnList = orderColumnList :+ asc_nulls_first(key)
        }
        if (value.toUpperCase() == Constant.SORT_DESC) {
          orderColumnList = orderColumnList :+ desc_nulls_last(key)
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
    df.show()
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

    if (!sparkSqlCondition.getDimensionIsExists) {
      compareValues = df1.coalesce(defaultNumPartitions).select(pivotsAlias).orderBy(asc(pivotsAlias)).distinct().limit(compareLimit).rdd.map(r => r.get(0).toString).collect().toList
    }
    else {
      compareValues = df1.coalesce(defaultNumPartitions).select(pivotsAlias).distinct().limit(compareLimit).rdd.map(r => r.get(0).toString).collect().toList
    }
    logger.info(s"【SQLExtInterpreter-日志】-【sparkSqlPivot】-对比列的值：${JsonUtil.objectToJson(compareValues)}")

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
    var aggList: List[Column] = sparkAgg(sparkAggMap)
    if ((aggList != null && aggList.nonEmpty) || sparkSqlCondition.getComparePctFlag) {
      var pivotFlag = false
      //对比项反转
      if (groupList != null && groupList.nonEmpty) {
        //百分比计算
        if (sparkSqlCondition.getComparePctFlag) {
          //拼接百分比表达式
          for ((k: String, v: String) <- sparkSqlCondition.getPctMap.asScala) {
            aggList = aggList :+ min(col(k) / col(v)).alias(k.substring(0, k.length - 5))
          }
          df1 = df1.groupBy(groupList.head, groupList.tail: _*).pivot(pivotsAlias, compareValues).agg(aggList.head, aggList.tail: _*)
          pivotFlag = true
        }
        else {
          df1 = df1.groupBy(groupList.head, groupList.tail: _*).pivot(pivotsAlias, compareValues).agg(aggList.head, aggList.tail: _*)
        }
        pivotFlag = true
      }
      else {
        df1 = df1.groupBy().pivot(pivotsAlias, compareValues).agg(aggList.head, aggList.tail: _*)
        pivotFlag = true
      }
      //交叉表排序
      if (!crosstabMap.isEmpty && pivotFlag) {
        df1 = handleSortAndLimitResult(df1, sparkSqlCondition)
      }
      if (pivotFlag) {
        df1 = df1.limit(sparkSqlCondition.getLimit)
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

    logger.info(s"【SQLExtInterpreter-日志】-【combineCompareColumn】-对比列：${JsonUtil.objectToJson(compareList)}")
    logger.info(s"【SQLExtInterpreter-日志】-【combineCompareColumn】-查询项：${JsonUtil.objectToJson(selectList)}")

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

  /**
    * 保存日志到mongodb
    *
    * @param log 日志对象
    **/
  def saveSqlExtLog(log: SqlExtInterpreterLog, sparkSqlCondition: SparkSqlCondition): Unit = {
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

  private[this] def newSparkSession(sparkSqlCondition: SparkSqlCondition): SparkSession = {

    var sparkConf = new SparkConf()
      .set("spark.default.parallelism", "1")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.executor.instances", "1")
      .set("spark.driver.cores", "1")
      .set("spark.executor.cores", "1")
      .setMaster("local[8]")
      .setAppName("spark_sql_default")

    var sparkConfMap: mutable.Map[String, String] = sparkSqlCondition.getSparkConfig.asScala
    if (sparkConfMap != null && sparkConfMap.nonEmpty) {
      val coalesce = "spark.custom.coalesce"
      if (sparkConfMap.contains(coalesce)) {
        defaultNumPartitions = Integer.valueOf(sparkConfMap(coalesce))
        sparkConfMap = sparkConfMap.-(coalesce)
      }
      sparkConfMap.keys.foreach(key => sparkConf.set(key, sparkConfMap(key)))
    }
    //构造spark session
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    sparkSession
    //数据源为hive
    //    var sessionStrategy: ISessionStrategy = new SparkSessionStrategy(sparkConf)
    //
    //    //    数据源为kudu
    //    if (sparkSqlCondition.getDataSourceType == 1) {
    //      sessionStrategy = new SparkKuduStrategy(sparkConf)
    //    }
    //    val con: ContextSessionStrategy = new ContextSessionStrategy(sessionStrategy)
    //    con.getSparkSession(sparkSqlCondition)
  }
}


object DataFrameExtensions {
  implicit def extendedDataFrame(dataFrame: DataFrame): ExtendedDataFrame =
    new ExtendedDataFrame(dataFrame: DataFrame)

  class ExtendedDataFrame(dataFrame: DataFrame) {
    def isEmpty: Boolean = {
      Try {
        dataFrame.first.length != 0
      } match {
        case Success(_) => false
        case Failure(_) => true
      }
    }

    def nonEmpty(): Boolean = !isEmpty
  }

}
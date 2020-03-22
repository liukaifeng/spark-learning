package com.lkf.v3

import java.io.StringWriter
import java.lang.reflect.InvocationTargetException
import java.util
import java.util.Objects

import cn.com.tcsl.cmp.client.dto.QueryTypeEnum
import cn.com.tcsl.cmp.client.dto.report.condition.{Constant, DateUtils, QoqDTO, SparkSqlBuild, SparkSqlCondition}
import cn.com.tcsl.cmp.client.dto.report.condition.Constant._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.mongodb.casbah.{MongoClient, MongoCredential}
import com.mongodb.util.JSON
import com.mongodb.{DBCollection, DBObject, ServerAddress}
import org.apache.kudu.spark.kudu._
import org.apache.livy.client.ext.SparkExtParam
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
  //非数值类型空值填充值
  private val nonNumFillValue = "- -"
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

    val param = "{\"cassandraFilter\":\"group_code in (3297,9759)\",\"compareFieldTypeMap\":{\"ljc_compare_x_place_order_timeweek1584429128000_0\":\"datetime\"},\"compareList\":[\"ljc_compare_x_place_order_timeweek1584429128000_0\"],\"comparePctFlag\":false,\"compareSortFlag\":true,\"crosstabByMap\":{},\"dataSourceType\":1,\"delFilterField\":false,\"dimensionIsExists\":true,\"fieldAliasAndDescMap\":{},\"filterCustomFieldList\":[],\"filterFormula\":\"\",\"groupList\":[],\"hiveJdbcConfig\":{\"hivePassword\":\"\",\"hiveUrl\":\"jdbc:hive2://192.168.12.203:21050/%s;auth=noSasl\",\"preExecuteSqls\":\"\",\"hiveUser\":\"root\"},\"indexList\":[\"ljc_sum_x_people_qtyint1583467517000_0\"],\"keyspace\":\"e000\",\"kuduMaster\":\"hadoop207\",\"limit\":1500,\"mongoConfigMap\":{\"mongoHost\":\"192.168.12.117\",\"mongoUserName\":\"lb\",\"mongoPort\":\"30017\",\"mongoPassword\":\"Lb#827\",\"mongoDb\":\"lb\"},\"page\":0,\"pctMap\":{},\"qoqList\":[],\"queryPoint\":0,\"queryType\":0,\"reverseByMap\":{},\"secondaryFlag\":true,\"selectList\":[\"ljc_compare_x_place_order_timeweek1584429128000_0\",\"ljc_sum_x_people_qtyint1583467517000_0\"],\"selectSql\":\"select   if(MONTH(place_order_time)= 12 && WEEKOFYEAR(place_order_time)=1,CONCAT(CAST(YEAR(YEARS_ADD(place_order_time, 1)) AS STRING), '年第', CAST(WEEKOFYEAR(place_order_time) AS STRING ), '周'),if(MONTH(place_order_time)= 1 && WEEKOFYEAR(place_order_time)>=52,CONCAT(CAST(YEAR(YEARS_SUB(place_order_time, 1)) AS STRING), '年第', CAST(WEEKOFYEAR(place_order_time) AS STRING ), '周'),CONCAT(CAST(YEAR(place_order_time) AS STRING), '年第', CAST(WEEKOFYEAR(place_order_time) AS STRING ), '周'))) as `ljc_compare_x_place_order_timeweek1584429128000_0` ,sum(CAST(people_qty AS DOUBLE)) as `ljc_sum_x_people_qtyint1583467517000_0` from e000.dw_trade_bill_detail_fact_p_group_test where group_code in (3297,9759) group by `ljc_compare_x_place_order_timeweek1584429128000_0`\",\"sessionId\":\"12\",\"sparkAggMap\":{\"sum\":[\"ljc_sum_x_people_qtyint1583467517000_0\"]},\"sparkConfig\":{\"spark.pyspark.driver.python\":\"./environment/bi_predict_python/bin/python\",\"spark.pyspark.python\":\"./environment/bi_predict_python/bin/python\",\"spark.executor.memory\":\"2000m\",\"spark.yarn.dist.archives\":\"hdfs://hadoop-slave2:6020/kaifeng/environment.tar.gz#environment\",\"spark.executor.cores\":\"1\",\"groupName\":\"group_report\",\"spark.driver.cores\":\"1\",\"spark.driver.extraJavaOptions\":\"-Dfile.encoding=utf8\",\"spark.scheduler.mode\":\"FAIR\",\"spark.sql.shuffle.partitions\":\"20\",\"spark.driver.memory\":\"2000m\",\"spark.executor.extraJavaOptions\":\"-Dfile.encoding=utf8\",\"spark.executor.instances\":\"1\",\"spark.custom.coalesce\":\"1\",\"spark.yarn.appMasterEnv.PYSPARK_PYTHON\":\"./environment/bi_predict_python/bin/python\",\"spark.default.parallelism\":\"20\"},\"table\":\"dw_trade_bill_detail_fact_p_group_test\",\"tracId\":\"15844291290008249440755\"}"
    var sparkConf = new SparkConf()
      .set("spark.default.parallelism", "1")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.executor.instances", "1")
      .set("spark.driver.cores", "1")
      .set("spark.executor.cores", "1")
      .set("spark.extraListeners", "com.lkf.v3.MySparkAppListener")
      .setMaster("local[2]")
      .setAppName("spark_sql_default")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //1-组装spark sql
    val sparkSqlCondition: SparkSqlCondition = new SparkExtParam().analyzeParam(param)
    sqlExtLog.sessionId = sparkSqlCondition.getSessionId
    sqlExtLog.tracId = sparkSqlCondition.getTracId

    try {
      val sqlStr: String = sparkSqlCondition.getSelectSql
      val sqlQoqStr: String = sparkSqlCondition.getSelectQoqSql
      logger.info(s"【SQLExtInterpreter-日志】-【execute】-主体SQL：$sqlStr")
      logger.info(s"【SQLExtInterpreter-日志】-【execute】-同环比SQL：$sqlQoqStr")

      //3-主体SQL执行
      val sqlExecuteBeginTime = System.currentTimeMillis()
      var df: DataFrame = null
      if (sqlStr.nonEmpty) {
        sqlExtLog.mainSql = sqlStr
        if (sparkSqlCondition.getSecondaryFlag) {
          //2-上下文添加配置
          var sparkSession: SparkSession = spark
          df = HiveJdbcUtil.execute2DataFrame(sparkSession, sparkSqlCondition, sqlStr)
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

      //9-分页处理
      //      if (sparkSqlCondition.getPage > 0) {
      //        val total = df.count()
      //        df = pagination(df, sparkSqlCondition, total)
      //      }

      // 10-获取数据结构
      val jsonString = df.schema.json
      val jSchema = parse(jsonString)
      // 11-获取数据
      val rows = df.take(sparkSqlCondition.getLimit).map(_.toSeq)
      implicit val formats = org.json4s.DefaultFormats.withBigDecimal
      val jRows = ExtractionUtil.decompose(rows.toArray.map(_.toSeq))
      df.unpersist()
    }
    catch {
      case e: InvocationTargetException =>
        val cause = e.getTargetException
        logger.error(s"Fail to execute query ", cause)
        sqlExtLog.msg = cause.toString
      case NonFatal(f) =>
        logger.error(s"Fail to execute query ", f)
        sqlExtLog.msg = f.toString
    }
    finally {
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
    df = df.orderBy(orderCols: _*)
    //    df = df.na.fill(fillValue).orderBy(orderCols: _*)
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
    val compareFieldTypeMap = sparkSqlCondition.getCompareFieldTypeMap
    for (compare <- compareList.asScala) {
      //对比项数据类型
      val compareDataType = compareFieldTypeMap.get(compare)
      //如果对比项类型是字符串，使用【- -】代替空值，否则使用【0】代替空值
      if (compareDataType.equals(DataFieldType.STRING_TYPE.getType) || compareDataType.equals(DataFieldType.DATETIME_TYPE.getType)) {
        compareCols = compareCols :+ when(col(compare).isNull, nonNumFillValue).when(col(compare).isin(""), nonNumFillValue).otherwise(col(compare))
      } else {
        compareCols = compareCols :+ when(col(compare).isNull, nonNumFillValue).otherwise(col(compare))
      }
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
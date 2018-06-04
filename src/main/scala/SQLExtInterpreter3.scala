import java.io.StringWriter
import java.util
import java.util.stream.Collectors

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.livy.client.ext.model.{Constant, SparkSqlBuild, SparkSqlCondition}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.{JavaConversions, mutable}

object SQLExtInterpreter3 {
  private val logger = LoggerFactory.getLogger(this.getClass)
  //对比项分隔符
  private val compareSplitChar = ":%"

  //逗号分隔符
  private val splitDot = ","

  //默认返回结果集条数
  private val defaultLimit = 1500

  //对比列值数量限制
  private val compareLimit = 1000

  //默认分区数量
  private val defaultNumPartitions = 4

  def main(args: Array[String]): Unit = {
    val begin = System.currentTimeMillis()
    System.setProperty("hadoop.home.dir", "D:\\Java\\hadoop-3.0")
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("spark_sql_hive_select")
      .config("hive.metastore.warehouse.dir", "/user/hive/warehouse")
      .config("hive.metastore.uris", "thrift://192.168.12.201:9083")
      .config("spark.default.parallelism", "2")
      .config("spark.sql.shuffle.partitions", "3")
      .config("spark.executor.instances ", "2")
      .enableHiveSupport()
      .getOrCreate()

    val param = "{\"compareCondition\":[{\"dataType\":\"str\",\"fieldDescription\":\"营业月\",\"fieldId\":\"180530114410006546\",\"fieldName\":\"settle_biz_month\",\"uniqId\":\"1527835767000\"},{\"dataType\":\"str\",\"fieldDescription\":\"营业周\",\"fieldId\":\"180530114410006548\",\"fieldName\":\"settle_biz_week\",\"uniqId\":\"1527836202000\"}],\"dbName\":\"cy7_1\",\"dimensionCondition\":[],\"filterCondition\":[{\"dataType\":\"str\",\"fieldDescription\":\"集团编号\",\"fieldId\":\"16\",\"fieldName\":\"group_code\",\"fieldValue\":[\"1008-JT004\"]}],\"indexCondition\":[{\"aggregator\":\"sum\",\"aggregatorName\":\"求和\",\"aliasName\":\"营业额（应收）\",\"dataType\":\"double\",\"fieldDescription\":\"营业额（应收）\",\"fieldId\":\"180530114409006519\",\"fieldName\":\"recv_money\",\"uniqId\":\"1527835762000\"}],\"limit\":10,\"page\":1,\"queryPoint\":1,\"queryType\":0,\"reportCode\":\"180601145137003193\",\"sortCondition\":[{\"dataType\":\"double\",\"fieldDescription\":\"营业额（应收）\",\"fieldId\":\"180530114409006519\",\"fieldName\":\"recv_money\",\"sortFlag\":\"asc\",\"sortType\":0}],\"tbId\":\"180530114409000181\",\"tbName\":\"dw_trade_bill_fact_p_group\"}"
    //组装spark sql
    val sparkSqlCondition: SparkSqlCondition = new SparkSqlBuild().buildSqlStatement(param)
    logger.info(s"【SQLExtInterpreter-日志】-【execute】-查询条件对象hash：${sparkSqlCondition.hashCode()}")
    logger.info(s"【SQLExtInterpreter-日志】-【execute】-查询条件拼接结果：${objectToJson(sparkSqlCondition)}")
    //    val sqlStr: String = sparkSqlCondition.getSelectSql
    val sqlStr: String = "SELECT\n  SUM(recv_money) AS recv_money,\n  settle_biz_month || ':%' || settle_biz_week AS Y\nFROM\n  cy7_1.dw_trade_bill_fact_p_group\nWHERE group_code = '1008-JT004'\nGROUP BY Y"
    logger.info(s"【SQLExtInterpreter-日志】-【execute】-完整SQL：$sqlStr")

    //SQL执行结果
    var df: DataFrame = spark.sql(sqlStr)

    df.sort(asc("y")).show()

    //    df.show()
    //    logger.info(s"【SQLExtInterpreter-日志】-【execute】-SQL查询数据量：${df.count()}")
    //
    //    val resultDf = useSparkApiAgg(df.coalesce(1), sparkSqlCondition)
    //    resultDf.getResult().show()
    //    logger.info(s"【SQLExtInterpreter-日志】-【execute】-聚合执行完毕==============}")
    //    var responseResult: ResponseResult = new ResponseResult
    //    if (resultDf.getMsg().nonEmpty) {
    //      responseResult.setMsg(resultDf.getMsg())
    //    }
    //    // 结果集解析
    //    parseResult(resultDf.getResult(), sparkSqlCondition)
  }


  //结果解析
  private[this] def parseResult(result: DataFrame, sparkSqlCondition: SparkSqlCondition): ResponseResult = {
    logger.info(s"【SQLExtInterpreter-日志】-【parseResult】-解析查询结果")
    //维度条件
    var dimensionList: util.List[String] = sparkSqlCondition.getDimensionList
    //指标条件
    var indexList: util.List[String] = sparkSqlCondition.getIndexList
    //字段名与中文名映射关系
    val fieldMap: util.Map[String, String] = sparkSqlCondition.getFieldMap
    //返回结果数量
    var limit: Int = defaultLimit
    if (sparkSqlCondition.getLimit > 0) {
      limit = sparkSqlCondition.getLimit
    }
    logger.info(s"【SQLExtInterpreter-日志】-【parseResult】-返回结果数量限制: $limit")

    //结果标题
    val arrayNames = result.columns
    logger.info(s"【SQLExtInterpreter-日志】-【parseResult】-列名: ${objectToJson(arrayNames)}")
    //结果数据
    val rows = result.getClass.getMethod("take", classOf[Int])
      .invoke(result, limit: Integer)
      .asInstanceOf[Array[Row]]
      .map(_.toSeq)
    logger.info(s"【SQLExtInterpreter-日志】-【parseResult】-结果数据rows: ${objectToJson(rows)}")

    var xAxisList: List[XAxis] = List()
    var yAxisList: List[YAxis] = List()
    val splitDot = ","
    for (nameIndex <- arrayNames.indices) {
      var stringBuilder = new StringBuilder
      //数据集列名
      var columnName = arrayNames.apply(nameIndex)
      //数据集列名的中文名称
      val fieldAlias = fieldMap.get(columnName)
      //      logger.info(s"【SQLExtInterpreter-日志】-【parseResult】-列名：$columnName")
      var key = parseCompareTitle(columnName, indexList, fieldMap)
      //      logger.info(s"【SQLExtInterpreter-日志】-【parseResult】-对比反转列名：$key")
      for (row <- rows.indices) {
        val rowArray = rows.apply(row)
        if (rowArray.apply(nameIndex) == null) {
          stringBuilder ++= "null".concat(splitDot)
        }
        else {
          stringBuilder ++= rowArray.apply(nameIndex).toString.concat(splitDot)
        }
      }
      var data: List[String] = List()
      if (stringBuilder.nonEmpty) {
        data = stringBuilder.substring(0, stringBuilder.length - splitDot.length).split(splitDot).toList
      }
      if (dimensionList != null && !dimensionList.isEmpty && dimensionList.contains(columnName)) {
        var xAxis: XAxis = new XAxis
        if (fieldAlias != null && fieldAlias.nonEmpty) {
          xAxis.name = fieldAlias
        }
        else {
          xAxis.name = key
        }
        xAxis.data = data
        val temp = xAxisList :+ xAxis
        xAxisList = temp
      }
      else {
        var yAxis: YAxis = new YAxis
        if (fieldAlias != null && fieldAlias.nonEmpty) {
          yAxis.name = fieldAlias
        }
        else {
          yAxis.name = key
        }
        yAxis.data = formatDouble(data)
        val temp = yAxisList :+ yAxis
        yAxisList = temp
      }
    }
    //对比结果分组
    val yAxisValues = parseCompareSort(yAxisList, sparkSqlCondition)
    var response = new ResponseResult
    response.setXAxis(xAxisList).setYAxis(yAxisValues)
    logger.info(s"【SQLExtInterpreter-日志】-【parseResult】-xAxisList结果: ${objectToJson(xAxisList)}")
    logger.info(s"【SQLExtInterpreter-日志】-【parseResult】-yAxisList结果: ${objectToJson(yAxisValues)}")
    logger.info(s"【SQLExtInterpreter-日志】-【parseResult】-response结果: ${objectToJson(response)}")
    response
  }

  //对比结果分组
  private[this] def parseCompareSort(yAxisList: List[YAxis], sparkSqlCondition: SparkSqlCondition): List[YAxis] = {
    //对比条件
    val compareList = sparkSqlCondition.getCompareList
    //指标条件
    val indexList = sparkSqlCondition.getIndexList
    var yAxis = yAxisList
    //对比项不为空且指标字段数量大于1
    if (compareList != null && indexList != null && !compareList.isEmpty && !indexList.isEmpty && indexList.size() > 1) {
      //根据指标字段进行分组
      var groupResult = yAxis.groupBy(y => {
        y.name.split(compareSplitChar)(0)
      }).values.toList
      yAxis = List()
      //收集分组后的数据
      for (y <- groupResult) {
        val temp: List[YAxis] = yAxis ::: y
        yAxis = temp
      }
    }
    yAxis
  }

  //对比项结果标题转换
  private[this] def parseCompareTitle(compareTitle: String, fieldList: util.List[String], fieldMap: util.Map[String, String]): String = {
    var title: String = compareTitle
    if (title == "y") {
      return title
    }
    //单指标情况下
    if (fieldList.size() == 1) {
      val fieldAlias = fieldMap.get(fieldList.get(0))
      if (fieldAlias != null && fieldAlias.nonEmpty) {
        title = fieldAlias.concat(compareSplitChar).concat(compareTitle)
      }
      else {
        title = fieldList.get(0).concat(compareSplitChar).concat(compareTitle)
      }
    }
    else {
      //多指标情况
      for (field <- fieldList.asScala) {
        if (compareTitle.contains(field)) {
          var splitResult: Array[String] = compareTitle.replace(field, "placeholder").split("_")
          if (splitResult.length > 1) {
            val fieldAlias = fieldMap.get(field)
            if (fieldAlias != null && fieldAlias.nonEmpty) {
              title = fieldAlias.concat(compareSplitChar).concat(splitResult(0))
            } else {
              title = field.concat(compareSplitChar).concat(splitResult(0))
            }
          }
        }
      }
    }
    return title
  }

  //使用Spark Api 进行聚合运算
  private[this] def useSparkApiAgg(df: DataFrame, sparkSqlCondition: SparkSqlCondition): MethodResult[DataFrame] = {
    var aggResult = new MethodResult[DataFrame]
    var df1 = df
    //分组条件
    var groupList: List[String] = if (sparkSqlCondition.getGroupList != null) JavaConversions.asScalaBuffer(sparkSqlCondition.getGroupList).toList else Nil

    //聚合条件（sum,count,avg）
    var aggMap: Map[String, util.List[String]] = if (sparkSqlCondition.getAggMap != null) JavaConversions.mapAsScalaMap(sparkSqlCondition.getAggMap).toMap else null

    //排序条件
    var orderByMap: Map[String, util.List[String]] = if (sparkSqlCondition.getOrderMap != null) JavaConversions.mapAsScalaMap(sparkSqlCondition.getOrderMap).toMap else null

    //对比条件
    var compareList: List[String] = if (sparkSqlCondition.getCompareList != null) JavaConversions.asScalaBuffer(sparkSqlCondition.getCompareList).toList else Nil

    //聚合条件转换
    var aggColumnList: List[Column] = sparkSqlAgg(aggMap)

    //排序条件转换
    var orderColumnList: List[Column] = sparkSqlOrder(orderByMap)

    //对比列值的获取
    var compareValuesResult: MethodResult[List[String]] = sparkSqlPivot(df, compareList.asJava)

    //对比列的值
    var compareValues = compareValuesResult.getResult()

    //对比例别名
    val pivotAlias = Constant.PIVOT_ALIAS

    //筛选项的值查询,对结果集去重
    if (sparkSqlCondition.getQueryType == 1) {
      df1 = df.distinct()
    }
    //分组条件、聚合条件、对比条件不为空
    if (groupList.nonEmpty && aggColumnList.nonEmpty && compareValues.nonEmpty) {
      df1 = df1.groupBy(groupList.head, groupList.tail: _*).pivot(pivotAlias, compareValues).agg(aggColumnList.head, aggColumnList.tail: _*).toDF()
    }
    else if (groupList.isEmpty && aggColumnList.nonEmpty && compareValues.nonEmpty) {
      df1 = df1.groupBy().pivot(pivotAlias, compareValues).agg(aggColumnList.head, aggColumnList.tail: _*).toDF()
    }
    else if (groupList.nonEmpty && aggColumnList.nonEmpty) {
      //分组条件、聚合条件不为空
      df1 = df1.groupBy(groupList.head, groupList.tail: _*).agg(aggColumnList.head, aggColumnList.tail: _*).toDF()
    }
    else if (aggColumnList.nonEmpty) {
      //聚合条件不为空(单指标)
      df1 = df1.agg(aggColumnList.head, aggColumnList.tail: _*).toDF()
    }
    //无对比项的情况下,对结果集进行排序
    if (compareValues.isEmpty && orderColumnList.nonEmpty) {
      df1 = df1.sort(orderColumnList: _*).toDF()
    }
    aggResult.setResult(df1)
    return aggResult
  }

  //聚合条件转换
  private[this] def sparkSqlAgg(aggMap: Map[String, util.List[String]]): List[Column] = {
    var aggColumnList: List[Column] = Nil
    if (aggMap != null && aggMap.nonEmpty) {
      logger.info(s"【SQLExtInterpreter-日志】-【sparkSqlAgg】-aggColumnList结果: $aggMap")
      for (key <- aggMap.keys) {
        if (key.toUpperCase().eq(Constant.AGG_SUM)) {
          val aggs: mutable.Buffer[String] = aggMap(key).asScala
          for (agg <- aggs) {
            aggColumnList = aggColumnList :+ sum(agg).alias(agg)
          }
        }
        if (key.toUpperCase().eq(Constant.AGG_COUNT)) {
          val aggs: mutable.Buffer[String] = aggMap(key).asScala
          for (agg <- aggs) {
            aggColumnList = aggColumnList :+ count(agg).alias(agg)
          }
        }
        if (key.toUpperCase().eq(Constant.AGG_AVG)) {
          val aggs: mutable.Buffer[String] = aggMap(key).asScala
          for (agg <- aggs) {
            aggColumnList = aggColumnList :+ avg(agg).alias(agg)
          }
        }
      }
      logger.info(s"【SQLExtInterpreter-日志】-【sparkSqlAgg】-aggColumnList结果: $aggColumnList")
    }
    return aggColumnList
  }

  //对比项处理
  private[this] def sparkSqlPivot(df: DataFrame, compareList: util.List[String]): MethodResult[List[String]] = {
    var compareResult = new MethodResult[List[String]]
    var compareValues: List[String] = List()
    if (compareList != null && !compareList.isEmpty) {
      var pivotsAlias: String = compareList.parallelStream.collect(Collectors.joining(compareSplitChar))
      logger.info(s"【SQLExtInterpreter-日志】-【sparkSqlPivot】-对比项：" + pivotsAlias)
      pivotsAlias = Constant.PIVOT_ALIAS
      logger.info(s"【SQLExtInterpreter-日志】-【sparkSqlPivot】-对比项统一别名【y】")
      //对比列的值
      compareValues = df.select(pivotsAlias).distinct().sort(asc(pivotsAlias)).limit(compareLimit).rdd.map(r => r.get(0).toString).collect().toList
      logger.info(s"【SQLExtInterpreter-日志】-【sparkSqlPivot】-对比列的值：${objectToJson(compareValues)}")
    }
    compareResult.setResult(compareValues)
    compareResult
  }

  //排序条件转换
  private[this] def sparkSqlOrder(orderMap: Map[String, util.List[String]]): List[Column] = {
    var orderColumnList: List[Column] = Nil
    if (orderMap != null && orderMap.nonEmpty) {
      logger.info(s"【SQLExtInterpreter-日志】-【sparkSqlOrder】-orderMap: $orderMap")
      for (key <- orderMap.keys) {
        val aggs: mutable.Buffer[String] = orderMap(key).asScala
        for (agg <- aggs) {
          if (key.toUpperCase().eq(Constant.SORT_ASC)) {
            orderColumnList = orderColumnList :+ asc(agg)
          }
          if (key.toUpperCase().eq(Constant.SORT_DESC)) {
            orderColumnList = orderColumnList :+ desc(agg)
          }
        }
      }
      logger.info(s"【SQLExtInterpreter-日志】-【sparkSqlOrder】-orderColumnList: $orderColumnList")
    }
    return orderColumnList
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
  private[this] def sparkConfig(hiveContext: HiveContext, sparkSqlCondition: SparkSqlCondition): HiveContext = {
    val sparkConfMap: mutable.Map[String, String] = sparkSqlCondition.getSparkConfig.asScala
    if (sparkConfMap.nonEmpty) {
      sparkConfMap.keys.foreach(key => hiveContext.setConf(key, sparkConfMap.get(key).toString))
    }
    else {
      //没有配置信息使用默认值
      hiveContext.setConf("spark.default.parallelism", "6")
      hiveContext.setConf("spark.sql.shuffle.partitions", "1")
      hiveContext.setConf("spark.executor.instances", "4")
    }
    return hiveContext
  }

  //四舍五入，保留两位小数
  private[this] def formatDouble(originData: List[String]): List[String] = {
    var resultData: List[String] = List()
    if (originData.nonEmpty) {
      originData.foreach(data => {
        if (data != null && data != "null") {
          val regex = """(-)?(\d+)(\.\d*)?""".r
          var formatData: String = data
          //判断是否是数字
          if (regex.pattern.matcher(data.replace("E", "1")).matches()) {
            logger.debug(s"【SQLExtInterpreter-日志】-【formatDouble】-原始数值: $data")
            formatData = "%.2f".format(BigDecimal.apply(data).toDouble)
            logger.debug(s"【SQLExtInterpreter-日志】-【formatDouble】-格式化后数值: $formatData")
          }
          val temp: List[String] = resultData :+ formatData
          resultData = temp
        } else {
          val temp: List[String] = resultData :+ data
          resultData = temp
        }
      })
    }
    return resultData
  }

}

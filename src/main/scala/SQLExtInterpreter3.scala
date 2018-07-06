import java.io.StringWriter
import java.lang.reflect.InvocationTargetException
import java.util
import java.util.Objects

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.livy.client.ext.model.Constant.{ALIAS_SPLIT_PREFIX, ALIAS_SPLIT_SUFFIX, PIVOT_ALIAS}
import org.apache.livy.client.ext.model.{Constant, SparkSqlBuild, SparkSqlCondition}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.{JavaConversions, mutable}
import scala.util.control.NonFatal

object SQLExtInterpreter3 {
  private val logger = LoggerFactory.getLogger(this.getClass)
  //对比项分隔符
  private val compareSplitChar = Constant.COMPARE_SPLIT_CHAR

  //逗号分隔符
  private val splitDot = ","

  //默认返回结果集条数
  private val defaultLimit = 1500

  //对比列值数量限制
  private val compareLimit = 1000

  //默认分区数量
  private val defaultNumPartitions = 1

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

    val param = "{\"compareCondition\":[],\"dbName\":\"cy7_2\",\"dimensionCondition\":[{\"aliasName\":\"单价乘十减二\",\"dataType\":\"int\",\"fieldDescription\":\"单价乘十减二\",\"fieldFormula\":\"di5lie*10-2\",\"fieldId\":\"180706114658003832\",\"fieldName\":\"danjiachengshijianer247604\",\"isBuildAggregated\":2,\"udfType\":0,\"uniqId\":\"1530859023000\"}],\"filterCondition\":[],\"indexCondition\":[{\"aggregator\":\"sum\",\"aggregatorName\":\"求和\",\"aliasName\":\"单价\",\"dataType\":\"double\",\"fieldDescription\":\"单价\",\"fieldId\":\"180703151547003083\",\"fieldName\":\"di5lie\",\"isBuildAggregated\":0,\"udfType\":0,\"uniqId\":\"1530859033000\"}],\"limit\":1000,\"page\":1,\"queryPoint\":0,\"queryType\":0,\"reportCode\":\"180702144125000248\",\"sessionId\":\"3\",\"sortCondition\":[],\"tbId\":\"180703151547000165\",\"tbName\":\"jisuanziduanceshibiaoqingwushanchu_sheet1_9759\",\"tracId\":\"1530859039000\"}"
    //            val param = "{\"compareCondition\":[],\"dbName\":\"cy7_2\",\"dimensionCondition\":[],\"filterCondition\":[],\"indexCondition\":[{\"aggregator\":\"sum\",\"aggregatorName\":\"求和\",\"aliasName\":\"10单个数字\",\"dataType\":\"int\",\"fieldDescription\":\"10单个数字\",\"fieldFormula\":\"10\",\"fieldId\":\"180706135722003849\",\"fieldName\":\"10dangeshuzi786605\",\"isBuildAggregated\":2,\"udfType\":0,\"uniqId\":\"1530856995000\"}],\"limit\":1000,\"page\":1,\"queryPoint\":0,\"queryType\":0,\"reportCode\":\"180702144125000248\",\"sessionId\":\"3\",\"sortCondition\":[],\"tbId\":\"180703151547000165\",\"tbName\":\"zizhibiaowushan_sheet1_9759\",\"tracId\":\"1530857041000\"}"

    try {
      //组装spark sql
      val sparkSqlCondition: SparkSqlCondition = new SparkSqlBuild().buildSqlStatement(param)

      logger.info(s"【SQLExtInterpreter-日志】-【execute】-查询条件拼接结果：${objectToJson(sparkSqlCondition)}")
      val sqlStr: String = sparkSqlCondition.getSelectSql
      //    val sqlStr: String = "SELECT\n  store_name,\n  SUM(recv_money) AS ljc_sum_0_recv_money,\n  SUM(cancel_money) AS ljc_sum_0_cancel_money\nFROM\n  cy7_1.dw_trade_bill_fact_p_group\nWHERE settle_biz_date = '2017-05-01'\n  AND group_code = '1008-JT004'\nGROUP BY store_name\nORDER BY ljc_sum_0_recv_money ASC"
      logger.info(s"【SQLExtInterpreter-日志】-【execute】-完整SQL：$sqlStr")
      var df: DataFrame = spark.sql(sqlStr)

      //自定义字段作为筛选项处理
      df = handleCustomField(df, sparkSqlCondition)
      df.show()
      //sql查询结果处理
      df = handleSqlDf(df, sparkSqlCondition)
      //对比项反转
      df = parseComparePivot(df.coalesce(defaultNumPartitions), sparkSqlCondition)

      //返回结果数量
      var limit: Int = defaultLimit
      //根据用户请求数量截取数据
      if (sparkSqlCondition.getLimit > 0) {
        limit = sparkSqlCondition.getLimit
        df = df.limit(limit)
      }
      df = handleLimitResult(df, sparkSqlCondition)

      // 结果集解析
      val result: ResponseResult = parseResult(df, sparkSqlCondition)
    } catch {
      case e: InvocationTargetException =>
        logger.info(s"Fail to execute query $param", e.getTargetException)
        val cause = e.getTargetException
        logger.info(s"Error ${cause.getMessage},${cause.getStackTrace.map(_.toString)}")
      case NonFatal(f) =>
        logger.info(s"Fail to execute query $param", f)
        logger.info(s"Error ${f.getMessage},${f.getStackTrace.map(_.toString)}")
    }
  }

  //取前后n条数据排序
  private[this] def handleLimitResult(limitDf: DataFrame, sparkSqlCondition: SparkSqlCondition): DataFrame = {
    var df = limitDf
    //对比条件
    val compareList: util.List[String] = sparkSqlCondition.getCompareList
    val orderListMap: util.Map[String, String] = sparkSqlCondition.getOrderByMap
    val crosstabOrderMap: util.Map[String, String] = sparkSqlCondition.getCrosstabByMap
    //不存在对比项且取数据方式为取后n条
    if (Objects.isNull(compareList) || compareList.isEmpty) {
      if (sparkSqlCondition.getQueryPoint == 2) {
        if (Objects.nonNull(orderListMap) && !orderListMap.isEmpty) {
          val orderCols = sparkOrderCols(orderListMap)
          df = df.orderBy(orderCols: _*)
        }
      }
      //交叉表只有维度和指标
      if (Objects.nonNull(crosstabOrderMap) && !crosstabOrderMap.isEmpty) {
        val orderCols = sparkOrderCols(crosstabOrderMap)
        df = df.orderBy(orderCols: _*)
      }
    }
    df
  }

  //sql查询结果集处理
  private[this] def handleSqlDf(sqlDf: DataFrame, sparkSqlCondition: SparkSqlCondition): DataFrame = {
    var df = sqlDf
    //指标条件
    val indexList: util.List[String] = sparkSqlCondition.getIndexList
    //维度条件
    val groupList: util.List[String] = sparkSqlCondition.getGroupList
    //对比条件
    val compareList: util.List[String] = sparkSqlCondition.getCompareList

    //筛选项的值查询,对结果集去重
    if (sparkSqlCondition.getQueryType == 1) {
      if (indexList != null) {
        df = df.distinct().where(indexList.get(0).concat("!=''"))
      }
    }
    //    //根据请求指定排序
    //    val orderByList: List[Column] = sparkOrderCols(sparkSqlCondition.getOrderByMap)
    //    if (orderByList != null && orderByList.nonEmpty) {
    //      df = df.orderBy(orderByList: _*)
    //    }
    //返回结果数量
    var limit: Int = defaultLimit
    //根据用户请求数量截取数据
    if (indexList != null && compareList != null && (groupList == null || groupList.isEmpty)) {
      if (sparkSqlCondition.getLimit > 0) {
        limit = sparkSqlCondition.getLimit
        df = df.limit(limit)
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
    return orderColumnList
  }

  //结果解析
  private[this] def parseResult(result: DataFrame, sparkSqlCondition: SparkSqlCondition): ResponseResult = {
    //分组项
    var groupList: List[String] = if (sparkSqlCondition.getGroupList != null) JavaConversions.asScalaBuffer(sparkSqlCondition.getGroupList).toList else Nil

    //指标项
    var indexList: List[String] = if (sparkSqlCondition.getIndexList != null) JavaConversions.asScalaBuffer(sparkSqlCondition.getIndexList).toList else Nil

    //对比项
    var compareList: List[String] = if (sparkSqlCondition.getCompareList != null) JavaConversions.asScalaBuffer(sparkSqlCondition.getCompareList).toList else Nil

    //查询项字段与中文名对应关系
    var fieldMap: util.Map[String, String] = sparkSqlCondition.getFieldAndDescMap

    //结果标题
    val arrayNames = result.columns
    logger.info(s"【SQLExtInterpreter-日志】-【parseResult】-列名: ${objectToJson(arrayNames)}")
    //结果数据
    val rows = result.getClass.getMethod("take", classOf[Int])
      .invoke(result, defaultLimit: java.lang.Integer)
      .asInstanceOf[Array[Row]]
      .map(_.toSeq)

    //x轴数据集
    var xAxisList: List[XAxis] = List()
    //y轴数据集
    var yAxisList: List[YAxis] = List()
    //龙决策首字母使用&隔开作为分隔符
    val splitDot = "L&J&C"
    for (nameIndex <- arrayNames.indices) {
      var stringBuilder = new StringBuilder
      for (row <- rows.indices) {
        val rowArray = rows.apply(row)
        if (rowArray.apply(nameIndex) == null) {
          stringBuilder ++= "0".concat(splitDot)
        }
        else {
          stringBuilder ++= rowArray.apply(nameIndex).toString.concat(splitDot)
        }
      }
      var data: List[String] = Nil
      if (stringBuilder.nonEmpty) {
        data = stringBuilder.substring(0, stringBuilder.length - splitDot.length).split(splitDot).toList
      }
      //数据集列名
      var columnName = arrayNames.apply(nameIndex)
      if (groupList != null && groupList.nonEmpty && groupList.contains(columnName)) {
        var xAxis: XAxis = new XAxis
        //列名的中文名称
        var key = parseCompareTitle(columnName, indexList, fieldMap, true, compareList)
        xAxis.name = key
        xAxis.data = formatDouble(data)
        val temp = xAxisList :+ xAxis
        xAxisList = temp
      }
      else if (indexList.nonEmpty || compareList.nonEmpty) {
        //列名的中文名称
        var key = parseCompareTitle(columnName, indexList, fieldMap, false, compareList)
        var yAxis: YAxis = new YAxis
        yAxis.name = key
        yAxis.data = formatDouble(data)
        val temp = yAxisList :+ yAxis
        yAxisList = temp
      }
    }
    //对比结果分组
    val yAxisValues = parseCompareTitleGroup(yAxisList, fieldMap.values(), sparkSqlCondition)
    var response = new ResponseResult
    response.setXAxis(xAxisList).setYAxis(yAxisValues)
    logger.info(s"【SQLExtInterpreter-日志】-【parseResult】-xAxisList结果: ${objectToJson(xAxisList)}")
    logger.info(s"【SQLExtInterpreter-日志】-【parseResult】-yAxisList结果: ${objectToJson(yAxisValues)}")
    logger.info(s"【SQLExtInterpreter-日志】-【parseResult】-response结果: ${objectToJson(response)}")
    response
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
      df = df.drop(col("ljc_filter_0_wen11ren806767"))
    }
    df
  }

  //对比结果标题分组
  private[this] def parseCompareTitleGroup(yAxisList: List[YAxis], fieldAlias: util.Collection[String], sparkSqlCondition: SparkSqlCondition): List[YAxis] = {
    //对比条件
    val compareList = if (sparkSqlCondition.getCompareList != null) JavaConversions.asScalaBuffer(sparkSqlCondition.getCompareList).toList else Nil

    //指标项
    var indexList: List[String] = if (sparkSqlCondition.getIndexList != null) JavaConversions.asScalaBuffer(sparkSqlCondition.getIndexList).toList else Nil

    //指标字段中文名
    val alias: Iterable[String] = fieldAlias.asScala

    //待分组数据集
    var yAxis = yAxisList

    //对比项不为空且指标字段数量大于1
    if (compareList != null && indexList != null && compareList.nonEmpty && indexList.nonEmpty && indexList.size > 1) {
      //根据指标字段进行分组
      var groupResult = yAxis.groupBy(y => {
        y.name.split(compareSplitChar)(0)
      }).values.toList
      //清空原有数据
      yAxis = List()
      //收集分组后的数据
      if (alias != null && alias.nonEmpty) {
        //控制结果顺序与指标条件顺序一致
        for (alia <- alias) {
          for (y <- groupResult) {
            if (y.head.name.split(compareSplitChar)(0) == alia) {
              val temp: List[YAxis] = yAxis ::: y
              yAxis = temp
            }
          }
        }
      }
      else {
        for (y <- groupResult) {
          val temp: List[YAxis] = yAxis ::: y
          yAxis = temp
        }
      }
    }
    yAxis
  }

  //对比项反转处理
  private[this] def parseComparePivot(df: DataFrame, sparkSqlCondition: SparkSqlCondition): DataFrame = {
    var df1 = df
    //分组项
    var groupList: List[String] = if (sparkSqlCondition.getGroupList != null) JavaConversions.asScalaBuffer(sparkSqlCondition.getGroupList).toList else Nil
    //对比项
    var compareList: List[String] = if (sparkSqlCondition.getCompareList != null) JavaConversions.asScalaBuffer(sparkSqlCondition.getCompareList).toList else Nil
    //对比项
    var sparkAggMap: mutable.Map[String, util.List[String]] = sparkSqlCondition.getSparkAggMap.asScala
    //交叉表排序项
    var crosstabMap: util.Map[String, String] = sparkSqlCondition.getCrosstabByMap
    //对比项处理
    if (compareList == null || compareList.isEmpty) {
      return df1
    }
    var pivotsAlias: String = PIVOT_ALIAS
    var compareValues: List[String] = List()
    logger.info(s"【SQLExtInterpreter-日志】-【parseComparePivot】-对比列合并")

    //对比例合并
    df1 = combineCompareColumn(df1, sparkSqlCondition)
    //对比列的值
    compareValues = df1.select(pivotsAlias).distinct().sort(pivotsAlias).limit(compareLimit).rdd.map(r => r.get(0).toString).collect().toList
    compareValues = formatDouble(compareValues)
    logger.info(s"【SQLExtInterpreter-日志】-【sparkSqlPivot】-对比列的值：${objectToJson(compareValues)}")
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
      if (!crosstabMap.isEmpty && pivotFlag) {
        var orderList: List[Column] = sparkOrderCols(crosstabMap)
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
    return aggColumnList
  }

  //合并对比列,并设置别名为【y】
  private[this] def combineCompareColumn(df: DataFrame, sparkSqlCondition: SparkSqlCondition): DataFrame = {
    import org.apache.spark.sql.Column
    import org.apache.spark.sql.functions.{col, concat_ws}
    var df1 = df
    //对比项
    var compareList: util.List[String] = sparkSqlCondition.getCompareList
    //查询项
    var selectList: util.List[String] = sparkSqlCondition.getSelectList

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
      compareCols = compareCols :+ col(compare)
    }
    //多个对比项
    if (compareList.size() > 1) {
      colList = colList :+ concat_ws(compareSplitChar, compareCols: _*).as(PIVOT_ALIAS)
    } else {
      colList = colList :+ compareCols.head.as(PIVOT_ALIAS)
    }
    df1 = df1.select(colList: _*)
    return df1
  }

  //对比项结果标题转换
  private[this] def parseCompareTitle(compareTitle: String, fieldList: List[String], fieldMap: util.Map[String, String], xAxis: Boolean, compareList: List[String]): String = {
    var title: String = compareTitle
    if (compareTitle.endsWith(compareSplitChar)) {
      title = compareTitle.concat("未知")
    }
    title = parseFieldAlias(title)
    //维度字段
    if (xAxis || compareList.isEmpty) {
      val fieldAlias = fieldMap.get(title)
      if (fieldAlias != null && fieldAlias.nonEmpty) {
        title = fieldAlias
      }
      return title
    }
    //单指标情况下
    if (fieldList.size == 1) {
      val fieldAlias = fieldMap.get(fieldList.head)
      if (fieldAlias != null && fieldAlias.nonEmpty) {
        title = fieldAlias.concat(compareSplitChar).concat(title)
      }
      else {
        title = fieldList.head.concat(compareSplitChar).concat(title)
      }
      return title
    }
    //多指标情况
    for (field <- fieldList) {
      if (title.contains(field)) {
        var splitResult: Array[String] = title.replace(field, "placeholder").split("_")
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

    //无指标情况
    if (title == PIVOT_ALIAS && fieldList.isEmpty && compareList.nonEmpty) {
      title = ""
      for (compare <- compareList) {
        val fieldAlias = fieldMap.get(parseFieldAlias(compare))
        title += fieldAlias.concat(compareSplitChar)
      }
      title = title.substring(0, title.length - compareSplitChar.length)
    }
    return title
  }

  //从别名中提取原有字段名
  private[this] def parseFieldAlias(title: String): String = {
    var titleResult = title
    //    处理自定义别名的字段,字段中包括以ljc_和_0_
    if (title.contains(ALIAS_SPLIT_PREFIX) && title.contains(ALIAS_SPLIT_SUFFIX)) {
      val prefixIndex = title.indexOf(ALIAS_SPLIT_PREFIX)
      val suffixIndex = title.indexOf(ALIAS_SPLIT_SUFFIX)
      titleResult = title.substring(0, prefixIndex).concat(title.substring(suffixIndex + ALIAS_SPLIT_SUFFIX.length))
    }
    return titleResult
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
    logger.info(s"【SQLExtInterpreter-日志】-【sparkConfig】-spark配置项：$sparkConfMap")
    if (sparkConfMap != null && sparkConfMap.nonEmpty) {
      sparkConfMap.keys.foreach(key => hiveContext.setConf(key, sparkConfMap.get(key).toString))
    }
    else {
      //没有配置信息使用默认值
      hiveContext.setConf("spark.default.parallelism", "6")
      hiveContext.setConf("spark.sql.shuffle.partitions", "1")
      hiveContext.setConf("spark.executor.instances", "4")
    }
    hiveContext
  }

  //四舍五入，保留两位小数
  private[this] def formatDouble(originData: List[String]): List[String] = {
    var resultData: List[String] = List()
    if (originData.nonEmpty) {
      originData.foreach(data => {
        if (data != null && data != "null" && data.contains(".")) {
          val regex = """(-)?(\d+)(\.\d*)?""".r
          var formatData: String = data
          //判断是否是数字
          val digital1 = regex.pattern.matcher(data.replace("E", "1")).matches()
          val digital2 = regex.pattern.matcher(data.replace("E-", "1")).matches()
          if (digital1 || digital2) {
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
    resultData
  }

}

package com.lkf.v1

import java.io.StringWriter
import java.lang.reflect.InvocationTargetException
import java.util
import java.util.Objects

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.lkf.demo.YAxis
import org.apache.livy.client.ext.model.Constant.PIVOT_ALIAS
import org.apache.livy.client.ext.model._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
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
  private var defaultLimit = 1500

  //对比列值数量限制
  private val compareLimit = 1000

  //默认分区数量
  private var defaultNumPartitions = 1

  private val unknown = "未知"

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\Java\\hadoop-3.0")
    //同环比
    val param = "{\"compareCondition\":[{\"aliasName\":\"门店名称\",\"dataType\":\"str\",\"fieldDescription\":\"门店名称\",\"fieldGroup\":0,\"fieldId\":\"180620140418000824\",\"fieldName\":\"store_name\",\"isBuildAggregated\":0,\"udfType\":0,\"uniqId\":\"1535529064000\"}],\"dataSourceType\":0,\"dbName\":\"cy7_2\",\"dimensionCondition\":[{\"aliasName\":\"地址\",\"dataType\":\"str\",\"fieldDescription\":\"地址\",\"fieldGroup\":0,\"fieldId\":\"180620140418000833\",\"fieldName\":\"address\",\"isBuildAggregated\":0,\"udfType\":0,\"uniqId\":\"1535529489000\"}],\"filterCondition\":[{\"dataType\":\"str\",\"fieldDescription\":\"集团\",\"fieldGroup\":0,\"fieldName\":\"group_code\",\"fieldValue\":[\"9759\"],\"isBuildAggregated\":0,\"udfType\":0}],\"indexCondition\":[{\"aggregator\":\"sum\",\"aggregatorName\":\"求和\",\"aliasName\":\"就餐人数(求和)\",\"dataType\":\"int\",\"fieldDescription\":\"就餐人数\",\"fieldGroup\":0,\"fieldId\":\"180620140417000705\",\"fieldName\":\"people_qty\",\"isBuildAggregated\":0,\"qoqType\":0,\"udfType\":0,\"uniqId\":\"1535081456000\"}],\"limit\":10,\"page\":0,\"queryPoint\":1,\"queryType\":0,\"reportCode\":\"180824113248004200\",\"sessionId\":\"3\",\"sortCondition\":[{\"dataType\":\"int\",\"fieldDescription\":\"就餐人数\",\"fieldGroup\":0,\"fieldId\":\"180620140417000705\",\"fieldName\":\"people_qty\",\"isBuildAggregated\":0,\"sortFlag\":\"desc\",\"sortType\":0,\"udfType\":0}],\"sparkConfig\":{\"spark.executor.memory\":\"7273m\",\"spark.driver.memory\":\"7273m\",\"spark.driver.cores\":\"2\",\"spark.cassandra.connection.host\":\"192.168.12.33,192.168.12.203,192.168.12.204\",\"spark.custom.coalesce\":\"4\",\"spark.executor.cores\":\"2\",\"spark.sql.shuffle.partitions\":\"5\",\"spark.cassandra.connection.port\":\"9042\",\"spark.default.parallelism\":\"8\",\"spark.scheduler.mode\":\"FAIR\",\"spark.executor.instances\":\"3\"},\"tbId\":\"180620140417000008\",\"tbName\":\"dw_trade_bill_fact_p_group\",\"tracId\":\"1535529699000\"}"

    try {
      //组装spark sql
      val sparkSqlCondition: SparkSqlCondition = new SparkSqlBuild().buildSqlStatement(param)
      val spark: SparkSession = newSparkSession(sparkSqlCondition)
      val sqlStr: String = sparkSqlCondition.getSelectSql

      logger.info(s"【SQLExtInterpreter-日志】-【execute】-主体SQL：${sparkSqlCondition.getSelectSql}")

      var df: DataFrame = null
      if (sqlStr.nonEmpty) {
        df = spark.sql(sqlStr)
      }

      //自定义字段作为筛选项处理
      df = handleCustomField(df, sparkSqlCondition)

      //sql查询结果处理
      df = handleSqlDf(df, sparkSqlCondition)

      //对比项反转
      df = parseComparePivot(df.coalesce(defaultNumPartitions), sparkSqlCondition)

      val totalNum: Long = df.count()
      //分页
      df = pagination(df, sparkSqlCondition, totalNum)

      df = handleLimitResult(df, sparkSqlCondition)
      df.show()
      // 结果集解析
      //      val result: ResponseResult = parseResult(df, sparkSqlCondition, totalNum)

      //      import org.json4s._
      //      import org.json4s.JsonDSL._
      //      import org.json4s.jackson.JsonMethods._
      //      implicit val formats = org.json4s.DefaultFormats
      //      // Get the schema info
      //      val schema = df.getClass.getMethod("schema").invoke(df)
      //      val jsonString = schema.getClass.getMethod("json").invoke(schema).asInstanceOf[String]
      //      val jSchema = parse(jsonString)
      //
      //      // Get the row data
      //      val rows = df.getClass.getMethod("take", classOf[Int])
      //        .invoke(df, 1000: java.lang.Integer)
      //        .asInstanceOf[Array[Row]]
      //        .map(_.toSeq)
      //      val jRows = Extraction.decompose(rows)
      //
      //      val json = ("schema" -> jSchema) ~ ("detail" -> jRows)


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
        defaultLimit = Integer.MAX_VALUE
        df = df.distinct().where(indexList.get(0).concat("!=''"))
      }
    }

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

    //页面大小
    val limit = sparkSqlCondition.getLimit

    if (limit > 0 && totalNum > limit) {
      defaultLimit = limit
      //跳过之前页的数据
      var skipNum = 0
      if (page > 0) {
        skipNum = (page - 1) * limit
      }
      if (skipNum > 0) {
        val df1 = df.coalesce(1).limit(skipNum)
        dfResult = df.coalesce(1).except(df1).limit(limit)
      }
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

  //对比结果标题分组
  private[this] def parseCompareTitleGroup(yAxisList: List[YAxis], fieldAlias: util.Collection[String], sparkSqlCondition: SparkSqlCondition): List[YAxis] = {
    //对比条件
    val compareList = if (sparkSqlCondition.getCompareList != null) JavaConversions.asScalaBuffer(sparkSqlCondition.getCompareList).toList else Nil

    //指标项
    val indexList: List[String] = if (sparkSqlCondition.getIndexList != null) JavaConversions.asScalaBuffer(sparkSqlCondition.getIndexList).toList else Nil

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
    val groupList: List[String] = if (sparkSqlCondition.getGroupList != null) JavaConversions.asScalaBuffer(sparkSqlCondition.getGroupList).toList else Nil
    //对比项
    val compareList: List[String] = if (sparkSqlCondition.getCompareList != null) JavaConversions.asScalaBuffer(sparkSqlCondition.getCompareList).toList else Nil
    //对比项
    val sparkAggMap: mutable.Map[String, util.List[String]] = sparkSqlCondition.getSparkAggMap.asScala
    //交叉表排序项
    val crosstabMap: util.Map[String, String] = sparkSqlCondition.getCrosstabByMap
    //对比项处理
    if (compareList == null || compareList.isEmpty) {
      return df1
    }
    val pivotsAlias: String = PIVOT_ALIAS
    var compareValues: List[String] = List()
    logger.info(s"【SQLExtInterpreter-日志】-【parseComparePivot】-对比列合并")

    //对比例合并
    df1 = combineCompareColumn(df1, sparkSqlCondition)
    //对比列的值
    compareValues = df1.coalesce(1).select(pivotsAlias).distinct().limit(compareLimit).rdd.map(r => r.get(0).toString).collect().toList
    logger.info(s"【SQLExtInterpreter-日志】-【sparkSqlPivot】-对比列的值: ${objectToJson(compareValues)}")

    //对比列为数字时，保留两位小数
    compareValues = formatDouble(compareValues, "z", sparkSqlCondition.getQueryType)
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

  //对比项结果标题转换
  private[this] def parseCompareTitle(compareTitle: String, fieldList: List[String], fieldMap: util.Map[String, String], xAxis: Boolean, compareList: List[String]): String = {
    var title: String = compareTitle
    if (compareTitle.endsWith(compareSplitChar)) {
      title = compareTitle.concat(unknown)
    }

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
        val fieldAlias = fieldMap.get(compare)
        title += fieldAlias.concat(compareSplitChar)
      }
      title = title.substring(0, title.length - compareSplitChar.length)
    }
    title
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
    var sparkConfMap: mutable.Map[String, String] = sparkSqlCondition.getSparkConfig.asScala
    logger.info(s"【SQLExtInterpreter-日志】-【sparkConfig】-spark配置项：$sparkConfMap")
    if (sparkConfMap != null && sparkConfMap.nonEmpty) {
      val coalesce = "spark.custom.coalesce"
      if (sparkConfMap.contains(coalesce)) {
        defaultNumPartitions = Integer.valueOf(sparkConfMap(coalesce))
        sparkConfMap = sparkConfMap.-(coalesce)
      }
      sparkConfMap.keys.foreach(key => hiveContext.setConf(key, sparkConfMap(key)))
    }
    else {
      //没有配置信息使用默认值
      hiveContext.setConf("spark.default.parallelism", "6")
      hiveContext.setConf("spark.sql.shuffle.partitions", "1")
      hiveContext.setConf("spark.executor.instances", "4")
    }

    //连接cassandra会话初始化
    val dataSourceType = sparkSqlCondition.getDataSourceType
    if (dataSourceType == 1) {
      logger.info(s"【SQLExtInterpreter-日志】-【sparkConfig】-cassandra配置项：${hiveContext.getAllConfs}")

      val df: DataFrame = hiveContext
        .read
        .format("org.apache.spark.sql.cassandra")
        .options(Map(
          "table" -> sparkSqlCondition.getTable,
          "keyspace" -> sparkSqlCondition.getKeyspace
        ))
        .load()
      //        .filter(sparkSqlCondition.getCassandraFilter)
      val tempView = "temp_cassandra_table_".concat(System.currentTimeMillis().toString)

      df.createOrReplaceTempView(tempView)
      val sql = sparkSqlCondition.getSelectSql
      val newSql = sql.replace(sparkSqlCondition.getKeyspace.concat(".").concat(sparkSqlCondition.getTable), tempView)
      sparkSqlCondition.setSelectSql(newSql)
    }
    hiveContext
  }

  /**
    * 四舍五入，保留两位小数
    *
    * @param originData 待格式化数据集
    * @param axis       维度标识
    * @param queryType  查询类型（0-常规查询；1-筛选项查询）
    * @return 返回格式化后的数据集
    */
  private[this] def formatDouble(originData: List[String], axis: String, queryType: Int): List[String] = {
    var resultData: List[String] = List()
    if (originData.nonEmpty) {
      originData.foreach(data => {
        if (data != null && data != "null") {
          val regex = """(-)?(\d+)(\.\d*)?""".r
          var formatData: String = data
          //判断是否是数字
          val digital1 = regex.pattern.matcher(data.replace("E", "1")).matches()
          val digital2 = regex.pattern.matcher(data.replace("E-", "1")).matches()

          //小数进行四舍五入
          if (digital1 || digital2) {
            if (data.contains(".")) {
              formatData = "%.2f".format(BigDecimal.apply(data).toDouble)
            }
          } else if (axis == "y" && queryType != 1) {
            //非数字并且是指标项
            formatData = "0.00"
          }

          val temp: List[String] = resultData :+ formatData
          resultData = temp
        } else if (axis == "y") {
          val temp: List[String] = resultData :+ "0"
          resultData = temp
        } else {
          logger.debug(s"【SQLExtInterpreter-日志】-【formatDouble】-非指标项空值处理")
          val temp: List[String] = resultData :+ unknown
          resultData = temp
        }
      })
    }
    resultData
  }

  private[this] def newSparkSession(sparkSqlCondition: SparkSqlCondition): SparkSession = {
    var sparkConf = new SparkConf()
      .set("spark.cassandra.connection.host", "192.168.12.33")
      .set("spark.cassandra.connection.port", "9042")
      .set("hive.metastore.warehouse.dir", "/user/hive/warehouse")
      .set("hive.metastore.uris", "thrift://192.168.12.201:9083")
      .set("spark.default.parallelism", "1")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.executor.instances", "1")
      .set("spark.driver.cores", "1")
      .set("spark.executor.cores", "1")
      .setMaster("local")
      .setAppName("spark_sql_test")

    //    "spark.executor.memory": "7273m",
    //    "spark.driver.memory": "7273m",
    //    "spark.driver.cores": "2",
    //    "spark.cassandra.connection.host": "192.168.12.33,192.168.12.203,192.168.12.204",
    //    "spark.custom.coalesce": "4",
    //    "spark.executor.cores": "8",
    //    "spark.sql.shuffle.partitions": "3",
    //    "spark.cassandra.connection.port": "9042",
    //    "spark.default.parallelism": "8",
    //    "spark.scheduler.mode": "FAIR",
    //    "spark.executor.instances": "2"

    val sparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    if (sparkSqlCondition.getDataSourceType == 1) {
      val begin: Long = System.currentTimeMillis()
      logger.info(s"【SQLExtInterpreter-日志】-cassandra连接创建开始：$begin")
      var df: DataFrame = sparkSession
        .read
        .format("org.apache.spark.sql.cassandra")
        .options(Map(
          "table" -> sparkSqlCondition.getTable,
          "keyspace" -> sparkSqlCondition.getKeyspace
        ))
        .load()
      //        .filter(sparkSqlCondition.getCassandraFilter)

      logger.info(s"【SQLExtInterpreter-日志】-cassandra连接创建结束：${System.currentTimeMillis() - begin}")

      df.createOrReplaceTempView("temp_cassandra_table")

      var sql = sparkSqlCondition.getSelectSql
      var newSql = sql.replace(sparkSqlCondition.getKeyspace.concat(".").concat(sparkSqlCondition.getTable), "temp_cassandra_table")
      sparkSqlCondition.setSelectSql(newSql)
    }
    sparkSession
  }

}

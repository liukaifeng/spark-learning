import java.io.StringWriter
import java.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.livy.client.ext.model.Constant.PIVOT_ALIAS
import org.apache.livy.client.ext.model.{SparkSqlBuild, SparkSqlCondition}
import org.apache.spark.sql.functions.asc
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
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


    // 结果集解析
    val result: ResponseResult = parseResult(df, sparkSqlCondition)
    val end = System.currentTimeMillis()
    val time = end - begin
    logger.info(s"【SQLExtInterpreter-日志】-【execute】-耗时：$time")
  }


  //结果解析
  private[this] def parseResult(result: DataFrame, sparkSqlCondition: SparkSqlCondition): ResponseResult = {
    logger.info(s"【SQLExtInterpreter-日志】-【parseResult】-解析查询结果")
    var groupList: util.List[String] = sparkSqlCondition.getGroupList
    var indexList: util.List[String] = sparkSqlCondition.getIndexList
    //结果标题
    val arrayNames = result.columns
    val columnJson = objectToJson(arrayNames)
    logger.info(s"【SQLExtInterpreter-日志】-【parseResult】-列名: $columnJson")
    //结果数据
    val rows = result.getClass.getMethod("take", classOf[Int])
      .invoke(result, 1000: java.lang.Integer)
      .asInstanceOf[Array[Row]]
      .map(_.toSeq)

    var xAxisList: List[XAxis] = List()
    var yAxisList: List[YAxis] = List()
    val begin = System.currentTimeMillis()
    for (nameIndex <- arrayNames.indices) {
      var stringBuilder = new StringBuilder
      var key = arrayNames.apply(nameIndex)
      key = parseCompareTitle(key, indexList)
      logger.info(s"【SQLExtInterpreter-日志】-【parseResult】-组装map:$nameIndex $key")
      for (row <- rows.indices) {
        val rowArray = rows.apply(row)
        if (rowArray.apply(nameIndex) == null) {
          stringBuilder ++= "null".concat(",")
        }
        else {
          stringBuilder ++= rowArray.apply(nameIndex).toString.concat(",")
        }
      }
      val data = stringBuilder.substring(0, stringBuilder.length - 1).split(",").toList
      if (groupList != null && !groupList.isEmpty && groupList.contains(key)) {
        var xAxis: XAxis = new XAxis
        xAxis.name = key
        xAxis.data = data
        val temp = xAxisList :+ xAxis
        xAxisList = temp
      }
      else {
        var yAxis: YAxis = new YAxis
        yAxis.name = key
        yAxis.data = formatDouble(data)
        val temp = yAxisList :+ yAxis
        yAxisList = temp
      }
    }
    var response = new ResponseResult
    response.setXAxis(xAxisList).setYAxis(yAxisList)
    val end = System.currentTimeMillis()

    logger.info(s"【SQLExtInterpreter-日志】-【parseResult】-遍历结果耗时: ${end - begin}")
    logger.info(s"【SQLExtInterpreter-日志】-【parseResult】-xAxisList结果: ${objectToJson(xAxisList)}")
    logger.info(s"【SQLExtInterpreter-日志】-【parseResult】-yAxisList结果: ${objectToJson(yAxisList)}")
    logger.info(s"【SQLExtInterpreter-日志】-【parseResult】-response结果: ${objectToJson(response)}")
    response
  }


  //对比项反转处理
  private[this] def parseComparePivot(df: DataFrame, sparkSqlCondition: SparkSqlCondition): DataFrame = {
    var df1 = df
    //分组项
    var groupList: List[String] = if (sparkSqlCondition.getGroupList != null) JavaConversions.asScalaBuffer(sparkSqlCondition.getGroupList).toList else Nil
    //对比项
    var compareList: List[String] = if (sparkSqlCondition.getCompareList != null) JavaConversions.asScalaBuffer(sparkSqlCondition.getCompareList).toList else Nil

    //对比项处理
    if (compareList == null || compareList.isEmpty) {
      return df1
    }
    var pivotsAlias: String = PIVOT_ALIAS
    var compareValues: List[String] = List()
    //对比列的值
    compareValues = df.select(pivotsAlias).distinct().sort(asc(pivotsAlias)).limit(compareLimit).rdd.map(r => r.get(0).toString).collect().toList
    logger.info(s"【SQLExtInterpreter-日志】-【sparkSqlPivot】-对比列的值：${objectToJson(compareValues)}")
    //对比项反转
    if (groupList != null && groupList.nonEmpty) {
      df1 = df.groupBy(groupList.head, groupList.tail: _*).pivot(pivotsAlias, compareValues).sum(sparkSqlCondition.getSumList.asScala: _*)
    }
    else {
      df1 = df.groupBy().pivot(pivotsAlias, compareValues).sum(sparkSqlCondition.getSumList.asScala: _*)
    }
    df1
  }

  //对比项结果标题转换
  private[this] def parseCompareTitle(compareTitle: String, fieldList: util.List[String]): String = {
    var title: String = compareTitle
    for (field <- fieldList.asScala) {
      if (compareTitle.contains(field)) {
        var splitResult: Array[String] = compareTitle.replace(field, "placeholder").split("_")
        if (splitResult.length > 1) {
          title = splitResult(0).concat(compareSplitChar).concat(field)
        }
      }
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
    hiveContext
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
    resultData
  }

}

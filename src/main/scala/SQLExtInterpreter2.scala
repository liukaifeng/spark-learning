//import java.io.StringWriter
//import java.util
//import java.util.stream.Collectors
//
//import com.fasterxml.jackson.databind.ObjectMapper
//import com.fasterxml.jackson.module.scala.DefaultScalaModule
//import org.apache.livy.client.ext.model.{SparkSqlBuild, SparkSqlCondition}
//import org.apache.spark.sql.types.{StringType, StructField, StructType}
//import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
//import org.json4s.Extraction
//import org.json4s.jackson.JsonMethods.parse
//import org.slf4j.LoggerFactory
//import org.spark_project.guava.collect.ImmutableMap
//
//import scala.collection.JavaConverters._
//import scala.collection.mutable
//
//object SQLExtInterpreter2 {
//  private val logger = LoggerFactory.getLogger(this.getClass)
//  private val compareSplitChar = ":%"
//
//  def main(args: Array[String]): Unit = {
//    val begin = System.currentTimeMillis()
//    System.setProperty("hadoop.home.dir", "D:\\Java\\hadoop-3.0")
//    val spark = SparkSession
//      .builder()
//      .master("local")
//      .appName("spark_sql_hive_select")
//      .config("hive.metastore.warehouse.dir", "/user/hive/warehouse")
//      .config("hive.metastore.uris", "thrift://192.168.12.201:9083")
//      .config("spark.default.parallelism", "6")
//      .config("spark.sql.shuffle.partitions", "1")
//      .config("spark.executor.instances ", "4")
//      .enableHiveSupport()
//      .getOrCreate()
//
//    val param = "{\"compareCondition\":[{\"dataType\":\"str\",\"fieldDescription\":\"settle_biz_month\",\"fieldId\":\"30\",\"fieldName\":\"settle_biz_month\",\"uniqId\":\"1526989779000\"},{\"dataType\":\"str\",\"fieldDescription\":\"settle_biz_date\",\"fieldId\":\"87\",\"fieldName\":\"settle_biz_date\",\"uniqId\":\"1526989646000\"}],\"dbName\":\"cy7\",\"dimensionCondition\":[{\"aliasName\":\"shop_name\",\"dataType\":\"str\",\"fieldDescription\":\"shop_name\",\"fieldId\":\"77\",\"fieldName\":\"shop_name\",\"uniqId\":\"1526989564000\"}],\"filterCondition\":[{\"dataType\":\"str\",\"fieldDescription\":\"集团编号\",\"fieldId\":\"16\",\"fieldName\":\"center_id\",\"fieldValue\":[\"3217\"]}],\"indexCondition\":[{\"aggregator\":\"sum\",\"aggregatorName\":\"求和\",\"aliasName\":\"orig_total\",\"dataType\":\"double\",\"fieldDescription\":\"orig_total\",\"fieldId\":\"47\",\"fieldName\":\"orig_total\",\"uniqId\":\"1526989572000\"},{\"aggregator\":\"sum\",\"aggregatorName\":\"求和\",\"aliasName\":\"disc_total\",\"dataType\":\"double\",\"fieldDescription\":\"disc_total\",\"fieldId\":\"48\",\"fieldName\":\"disc_total\",\"uniqId\":\"1526989574000\"}],\"limit\":10,\"page\":1,\"queryPoint\":1,\"sessionId\":\"3\",\"sortCondition\":[],\"tbId\":\"t4\",\"tbName\":\"hdfs_src_cy7_biz_bs_wide_g\",\"tracId\":\"1526989779000\"}"
//    //组装spark sql
//    val sparkSqlCondition: SparkSqlCondition = SparkSqlBuild.buildSqlStatement(param)
//    //    val sqlStr: String = sparkSqlCondition.getSelectSql
//    val sqlStr: String = "SELECT\n  orig_total,\n  disc_total,\n  shop_name,\n  settle_biz_month || ':%' || settle_biz_date AS Y\nFROM\n  cy7.hdfs_src_cy7_biz_bs_wide_g\nWHERE center_id = '3217'\n  AND orig_total IS NOT NULL\n  AND disc_total IS NOT NULL\n  AND shop_name IS NOT NULL\n  AND settle_biz_month IS NOT NULL\n  AND settle_biz_date IS NOT NULL\nLIMIT 100"
//    logger.info(s"【SQLExtInterpreter-日志】-【execute】-sqlStr值：$sqlStr")
//
//    var df: DataFrame = spark.sql(sqlStr)
//    logger.info(s"【SQLExtInterpreter-日志】-【execute】-结果行数：${df.count()}")
//    //    logger.info(s"【SQLExtInterpreter-日志】-【execute】-sqlStr查询结果：${df.show(100)}")
//    var groupList: util.List[String] = sparkSqlCondition.getGroupList
//    var compareList: util.List[String] = sparkSqlCondition.getCompareList
//
//    logger.info(s"【SQLExtInterpreter-日志】-【execute】-分组项：" + objectToJson(groupList))
//
//    var map: Map[String, String] = Map("orig_total" -> "sum", "disc_total" -> "sum")
//    import org.apache.spark.sql.functions.{date_format, col, countDistinct, sum, desc,asc}
//    df.groupBy("shop_name", "y").agg(sum("orig_total").alias("orig_total"), sum("disc_total").alias("disc_total")).orderBy(asc("orig_total")).show(100)
//    //    //对比项处理
//    //    if (compareList != null && !compareList.isEmpty) {
//    //      var pivotsAlias: String = compareList.parallelStream().collect(Collectors.joining(compareSplitChar))
//    //      logger.info(s"【SQLExtInterpreter-日志】-【execute】-对比项：" + pivotsAlias)
//    //      pivotsAlias = "y"
//    //      logger.info(s"【SQLExtInterpreter-日志】-【execute】-对比项统一别名y：")
//    //
//    //      //对比列的值
//    //      var compareValuesDs: mutable.Buffer[Row] = df.select(pivotsAlias).distinct().collectAsList().asScala
//    //      var compareValues: List[String] = List()
//    //      for (row: Row <- compareValuesDs) {
//    //        val temp = compareValues :+ row.getAs[String](0)
//    //        compareValues = temp
//    //        logger.info(s"【SQLExtInterpreter-日志】-【execute】-对比列的值：${row.getAs[String](0)}")
//    //      }
//    //      logger.info(s"【SQLExtInterpreter-日志】-【execute】-对比列所有值：${objectToJson(compareValues)}")
//    //
//    //
//    //      //分组
//    //      if (!groupList.isEmpty && groupList.size() > 0) {
//    //        val firstGroup = groupList.get(0)
//    //        if (groupList != null && !groupList.isEmpty) {
//    //          val group = groupList.asScala
//    //          df = df.coalesce(1).groupBy(firstGroup, group.tail: _*).pivot(pivotsAlias, compareValues).sum(sparkSqlCondition.getSumList.asScala: _*)
//    //        }
//    //        else {
//    //          df = df.coalesce(1).groupBy(firstGroup).pivot(pivotsAlias, compareValues).sum(sparkSqlCondition.getSumList.asScala: _*)
//    //        }
//    //        logger.info(s"【SQLExtInterpreter-日志】-【execute】-对比列反转结果：${df.show(100)}")
//    //      }
//    //    }
//    //    //    parseCompareItemSort(df, spark)
//    //    val result: ResponseResult = parseResult(df, sparkSqlCondition)
//    //
//    //    val end = System.currentTimeMillis()
//    //    val time = end - begin
//    //    logger.info(s"【SQLExtInterpreter-日志】-【execute】-耗时：$time")
//    //    logger.info(s"【SQLExtInterpreter-日志】-【execute】-结果：${objectToJson(result)}")
//  }
//
//
//  //结果解析
//  private[this] def parseResult(result: DataFrame, sparkSqlCondition: SparkSqlCondition): ResponseResult = {
//    logger.info(s"【SQLExtInterpreter-日志】-【parseResult】-解析查询结果")
//    var groupList: util.List[String] = sparkSqlCondition.getGroupList
//
//    var indexList: util.List[String] = sparkSqlCondition.getIndexList
//    //结果标题
//    val arrayNames = result.columns
//    logger.info(s"【SQLExtInterpreter-日志】-【parseResult】-列名: ${objectToJson(arrayNames)}")
//    //结果数据
//    val rows = result.getClass.getMethod("take", classOf[Int])
//      .invoke(result, 1000: Integer)
//      .asInstanceOf[Array[Row]]
//      .map(_.toSeq)
//    logger.info(s"【SQLExtInterpreter-日志】-【parseResult】-结果数据rows: ${objectToJson(rows)}")
//
//    //    var map: TreeMap[String, List[String]] = TreeMap()
//    var xAxisList: List[XAxis] = List()
//    var yAxisList: List[YAxis] = List()
//    for (nameIndex <- arrayNames.indices) {
//      var stringBuilder = new StringBuilder
//      var key = arrayNames.apply(nameIndex)
//      key = parseCompareTitle(key, indexList)
//      for (row <- rows.indices) {
//        val rowArray = rows.apply(row)
//        if (rowArray.apply(nameIndex) == null) {
//          stringBuilder ++= "null".concat(",")
//        }
//        else {
//          stringBuilder ++= rowArray.apply(nameIndex).toString.concat(",")
//        }
//      }
//      val data = stringBuilder.substring(0, stringBuilder.length - 1).split(",").toList
//      logger.info(s"【SQLExtInterpreter-日志】-【parseResult】-结果遍历:$nameIndex 【名称】:$key,【数据】${objectToJson(data)}")
//      if (groupList != null && !groupList.isEmpty && groupList.contains(key)) {
//        var xAxis: XAxis = new XAxis
//        xAxis.name = key
//        xAxis.data = data
//        val temp = xAxisList :+ xAxis
//        xAxisList = temp
//      }
//      else {
//        var yAxis: YAxis = new YAxis
//        yAxis.name = key
//        yAxis.data = formatDouble(data)
//        val temp = yAxisList :+ yAxis
//        yAxisList = temp
//      }
//    }
//    var response = new ResponseResult
//    response.setXAxis(xAxisList).setYAxis(yAxisList)
//    logger.info(s"【SQLExtInterpreter-日志】-【parseResult】-xAxisList结果: ${objectToJson(xAxisList)}")
//    logger.info(s"【SQLExtInterpreter-日志】-【parseResult】-yAxisList结果: ${objectToJson(yAxisList)}")
//    logger.info(s"【SQLExtInterpreter-日志】-【parseResult】-response结果: ${objectToJson(response)}")
//    response
//  }
//
//  //对比项结果标题转换
//  private[this] def parseCompareTitle(compareTitle: String, fieldList: util.List[String]): String = {
//    var title: String = compareTitle
//    for (field <- fieldList.asScala) {
//      if (compareTitle.contains(field)) {
//        var splitResult: Array[String] = compareTitle.replace(field, "placeholder").split("_")
//        if (splitResult.length > 1) {
//          title = splitResult(0).concat(compareSplitChar).concat(field)
//        }
//      }
//    }
//    title
//  }
//
//  //对比项排序处理
//  private[this] def parseCompareItemSort(result: DataFrame, spark: SparkSession): DataFrame = {
//    logger.info(s"【SQLExtInterpreter-日志】-【parseCompareItemSort】-对比项排序,Methods：${objectToJson(result.getClass.getMethods)}")
//    // Get the schema info
//    val schema = result.getClass.getMethod("schema").invoke(result)
//    val jsonString = schema.getClass.getMethod("json").invoke(schema).asInstanceOf[String]
//    val jSchema = parse(jsonString)
//
//    logger.info(s"【SQLExtInterpreter-日志】-【parseCompareItemSort】-对比项排序,schema：${objectToJson(schema)}")
//    logger.info(s"【SQLExtInterpreter-日志】-【parseCompareItemSort】-对比项排序,jsonString：${jsonString}")
//
//
//    // Get the row data
//    val rows = result.getClass.getMethod("take", classOf[Int])
//      .invoke(result, 100: java.lang.Integer)
//      .asInstanceOf[Array[Row]]
//      .map(_.toSeq)
//    //    val jRows = Extraction.decompose(rows)
//    logger.info(s"【SQLExtInterpreter-日志】-【parseCompareItemSort】-对比项排序,数据集：${objectToJson(rows)}")
//    //    val rows: mutable.Buffer[Row] = df.collectAsList().asScala
//    //    logger.info(s"【SQLExtInterpreter-日志】-【parseCompareItemSort】-对比项排序,数据集：${objectToJson(rows)}")
//    //    for (row: Row <- rows) {
//    //      logger.info(s"【SQLExtInterpreter-日志】-【parseCompareItemSort】-对比项排序,数据行：${objectToJson(row)}")
//    //    }
//    //    logger.info(s"【SQLExtInterpreter-日志】-【parseCompareItemSort】-对比项排序,列标题：${df.columns}")
//    result
//  }
//
//  //转json字符串
//  private[this] def objectToJson(obj: Any): String = {
//    val mapper = new ObjectMapper()
//    mapper.registerModule(DefaultScalaModule)
//    val out = new StringWriter
//    mapper.writeValue(out, obj)
//    val json = out.toString
//    json
//  }
//
//  //四舍五入，保留两位小数
//  private[this] def formatDouble(originData: List[String]): List[String] = {
//    var resultData: List[String] = List()
//    if (originData.nonEmpty) {
//      originData.foreach(data => {
//        if (data != null && data != "null") {
//          val regex = """(-)?(\d+)(\.\d*)?""".r
//          var formatData: String = data
//          //判断是否是数字
//          if (regex.pattern.matcher(data.replace("E", "1")).matches()) {
//            //            logger.info(s"【SQLExtInterpreter-日志】-【formatDouble】-原始数值: $data")
//            formatData = "%.2f".format(BigDecimal.apply(data).toDouble)
//            //            logger.info(s"【SQLExtInterpreter-日志】-【formatDouble】-格式化后数值: $formatData")
//          }
//          val temp: List[String] = resultData :+ formatData
//          resultData = temp
//        }
//        else {
//          val temp: List[String] = resultData :+ data
//          resultData = temp
//        }
//      })
//    }
//    resultData
//  }
//}

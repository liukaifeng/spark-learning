//import java.io.StringWriter
//import java.util
//import java.util.stream.Collectors
//
//import com.fasterxml.jackson.databind.ObjectMapper
//import com.fasterxml.jackson.module.scala.DefaultScalaModule
//import org.apache.livy.client.ext.model.{SparkSqlBuild, SparkSqlCondition}
//import org.apache.spark.sql.{DataFrame, Row, SparkSession}
//import org.slf4j.LoggerFactory
//
//import scala.collection.JavaConverters._
//import scala.collection.immutable.TreeMap
//
//object SQLExtInterpreter {
//  private val logger = LoggerFactory.getLogger(this.getClass)
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
//      .config("fs.alluxio.impl", "alluxio.hadoop.FileSystem")
//      .enableHiveSupport()
//      .getOrCreate()
//
//    val param = "{\"compareCondition\":[],\"dbName\":\"cy7\",\"dimensionCondition\":[{\"aliasName\":\"\",\"dataType\":\"str\",\"fieldDescription\":\"shop_name\",\"fieldId\":\"77\",\"fieldName\":\"shop_name\",\"granularity\":\"\"},{\"aliasName\":\"\",\"dataType\":\"int\",\"fieldDescription\":\"province_id\",\"fieldId\":\"80\",\"fieldName\":\"province_id\",\"granularity\":\"\"}],\"filterCondition\":[{\"dataType\":\"str\",\"fieldDescription\":\"集团编号\",\"fieldId\":\"16\",\"fieldName\":\"center_id\",\"fieldValue\":[\"3217\"]}],\"indexCondition\":[{\"aggregator\":\"sum\",\"aggregatorName\":\"求和\",\"aliasName\":\"\",\"dataType\":\"double\",\"fieldDescription\":\"last_total\",\"fieldId\":\"49\",\"fieldName\":\"last_total\"},{\"aggregator\":\"sum\",\"aggregatorName\":\"求和\",\"aliasName\":\"\",\"dataType\":\"double\",\"fieldDescription\":\"disc_total\",\"fieldId\":\"48\",\"fieldName\":\"disc_total\"},{\"aggregator\":\"sum\",\"aggregatorName\":\"求和\",\"aliasName\":\"\",\"dataType\":\"double\",\"fieldDescription\":\"income_total\",\"fieldId\":\"50\",\"fieldName\":\"income_total\"}],\"limit\":10,\"page\":1,\"queryPoint\":1,\"sessionId\":\"3\",\"sortCondition\":[],\"tbId\":\"t4\",\"tbName\":\"hdfs_src_cy7_biz_bs_wide_g\",\"tracId\":\"1526462476000\"}"
//    //组装spark sql
//    val sparkSqlCondition: SparkSqlCondition = SparkSqlBuild.buildSqlStatement(param)
//    val sqlStr: String = sparkSqlCondition.getSelectSql
//    //    val sqlStr: String = "SELECT\n  item_orig_money,\n  item_last_money,\n  shop_name,\n  settle_biz_month,\n  settle_biz_date\nFROM\n  cy7.hdfs_src_cy7_biz_bs_wide_g\nWHERE center_id = '3217'\n  AND item_last_money IS NOT NULL\n  AND shop_name IS NOT NULL\n  AND item_orig_money IS NOT NULL\n  AND settle_biz_date IS NOT NULL\n  AND settle_biz_month IS NOT NULL\nLIMIT 100"
//    logger.info(s"【SQLExtInterpreter-日志】-【execute】-sqlStr值：$sqlStr")
//
//    var df: DataFrame = spark.sql(sqlStr)
//    logger.info(s"【SQLExtInterpreter-日志】-【execute】-sqlStr查询结果：${df.show(100)}")
//    var groupList: util.List[String] = sparkSqlCondition.getGroupList
//    var compareList: util.List[String] = sparkSqlCondition.getCompareList
//    logger.info(s"【SQLExtInterpreter-日志】-【execute】-分组项：" + objectToJson(groupList))
//    //对比项
//    val spiltChar: String = ":%"
//    if (compareList != null && !compareList.isEmpty) {
//      var pivots: String = compareList.parallelStream().collect(Collectors.joining(spiltChar))
//      logger.info(s"【SQLExtInterpreter-日志】-【execute】-对比项：" + pivots)
//      pivots = "y"
//      logger.info(s"【SQLExtInterpreter-日志】-【execute】-对比项统一别名y：")
//
//      //分组
//      if (!groupList.isEmpty && groupList.size() > 0) {
//        val firstGroup = groupList.get(0)
//        if (groupList != null && !groupList.isEmpty) {
//          val group = groupList.asScala
//
//          df = df.repartition(1).groupBy(firstGroup, group.tail: _*).pivot(pivots).sum(sparkSqlCondition.getSumList.asScala: _*)
//        }
//        else {
//          df = df.repartition(1).groupBy(firstGroup).pivot(pivots).sum(sparkSqlCondition.getSumList.asScala: _*)
//        }
//        import org.apache.spark.sql.functions.{date_format, col, countDistinct, sum, desc}
//        //        df.orderBy(desc("2017-01:%2017-01-01_sum(item_orig_money)")).show(100)
//        logger.info(s"【SQLExtInterpreter-日志】-【execute】-分组反转结果：${df.show(100)}")
//      }
//    }
//
//
//    //      df.show(false)
//    val result: ResponseResult = parseResult(df, sparkSqlCondition)
//    val end = System.currentTimeMillis()
//    val time = end - begin
//    logger.info(s"【SQLExtInterpreter-日志】-【execute】-耗时：$time")
//    logger.info(s"【SQLExtInterpreter-日志】-【execute】-结果：${objectToJson(result)}")
//
//  }
//
//
//  //结果解析
//  private[this] def parseResult(result: DataFrame, sparkSqlCondition: SparkSqlCondition): ResponseResult = {
//    logger.info(s"【SQLExtInterpreter-日志】-【parseResult】-解析查询结果")
//    var groupList: util.List[String] = sparkSqlCondition.getGroupList
//    var compareList: util.List[String] = sparkSqlCondition.getCompareList
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
//    var map: TreeMap[String, List[String]] = TreeMap()
//    for (nameIndex <- arrayNames.indices) {
//      var stringBuilder = new StringBuilder
//      logger.info(s"【SQLExtInterpreter-日志】-【parseResult】-组装map:$nameIndex ${arrayNames.apply(nameIndex)}")
//      for (row <- rows.indices) {
//        val rowArray = rows.apply(row)
//        if (rowArray.apply(nameIndex) == null) {
//          stringBuilder ++= "null".concat(",")
//        }
//        else {
//          stringBuilder ++= rowArray.apply(nameIndex).toString.concat(",")
//        }
//      }
//      if (stringBuilder.nonEmpty) {
//        val list = stringBuilder.substring(0, stringBuilder.length - 1).split(",").toList
//        map += (arrayNames.apply(nameIndex) -> list)
//
//        logger.info(s"【SQLExtInterpreter-日志】-【parseResult】-组装map:$nameIndex ${objectToJson(map)}")
//      }
//    }
//    val mapSize = map.size
//    logger.info(s"【SQLExtInterpreter-日志】-【parseResult】-遍历结果: ${objectToJson(map)}")
//    logger.info(s"【SQLExtInterpreter-日志】-【parseResult】-结果大小: $mapSize")
//    var xAxisList: List[XAxis] = List()
//    var yAxisList: List[YAxis] = List()
//
//    val begin = System.currentTimeMillis()
//    var counters = 0
//    logger.info(s"【SQLExtInterpreter-日志】-【parseResult】-groupList: ${objectToJson(groupList)}")
//
//    map.keys.foreach(key => {
//      var data = map(key)
//      logger.info(s"【SQLExtInterpreter-日志】-【parseResult】-key: $key,循环第 $counters 次")
//      logger.info(s"【SQLExtInterpreter-日志】-【parseResult】-data: $data")
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
//      val xsize = xAxisList.length
//      val ysize = yAxisList.length
//
//      counters = counters + 1
//    })
//
//    var response = new ResponseResult
//    response.setXAxis(xAxisList).setYAxis(yAxisList)
//
//    val x = objectToJson(xAxisList)
//    val y = objectToJson(yAxisList)
//    val res = objectToJson(response)
//    val end = System.currentTimeMillis()
//    val time = end - begin
//    logger.info(s"【SQLExtInterpreter-日志】-【parseResult】-遍历结果耗时: $time")
//    logger.info(s"【SQLExtInterpreter-日志】-【parseResult】-xAxisList结果: $x")
//    logger.info(s"【SQLExtInterpreter-日志】-【parseResult】-yAxisList结果: $y")
//    logger.info(s"【SQLExtInterpreter-日志】-【parseResult】-response结果: $res")
//    response
//  }
//
//  //  private[this] def parseNonCrosstabResult()(result: DataFrame, sparkSqlCondition: SparkSqlCondition): ResponseResult ={
//  //
//  //  }
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
//      })
//    }
//    resultData
//  }
//}

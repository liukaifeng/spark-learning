//import java.io.StringWriter
//import java.util
//import java.util.stream.Collectors
//
//import com.fasterxml.jackson.databind.ObjectMapper
//import com.fasterxml.jackson.module.scala.DefaultScalaModule
//import com.google.gson.Gson
//import com.spark.model.{BiReportBuildInDTO, Build, SparkSqlCondition}
//import org.apache.spark.sql._
//import org.json4s._
//
//import scala.collection.JavaConverters._
//
//object SparkHiveSelect2 {
//
//  case class ExecuteSuccess(content: JObject)
//
//  def main(args: Array[String]): Unit = {
//    System.setProperty("hadoop.home.dir", "D:\\Java\\hadoop-3.0")
//    val spark = SparkSession
//      .builder()
//      .master("local")
//      .appName("spark_sql_hive_select")
//      .config("hive.metastore.warehouse.dir", "/user/hive/warehouse")
//      .config("hive.metastore.uris", "thrift://192.168.12.201:9083")
//      .config("spark.sql.shuffle.partitions", 1)
//      .config("spark.default.parallelism", 2)
//      .enableHiveSupport()
//      .getOrCreate()
//
//    import spark.sql
//
//    var biReportBuildInDTO: BiReportBuildInDTO = new BiReportBuildInDTO()
//    val sparkSqlCondition: SparkSqlCondition = Build.buildSqlStatement("")
//    println(sparkSqlCondition.getSelectSql)
//    //    val sqlStr = "SELECT  shop_name,DATE_FORMAT(settle_biz_date, 'yyyy-MM-dd') AS settle_biz_date,sum(orig_total) as orig_total FROM  " +
//    //      " cy7.hdfs_src_cy7_biz_bs_wide WHERE settle_biz_date >= '2018-02-01'   AND settle_biz_date <= '2018-03-03'   AND center_id = '3217' " +
//    //      "group by shop_name,settle_biz_date"
//
//    var df = sql(sparkSqlCondition.getSelectSql)
//
//    var groupList: util.List[String] = sparkSqlCondition.getGroupList
//    var compareList: util.List[String] = sparkSqlCondition.getCompareList
//    //对比项
//    if (compareList != null && !compareList.isEmpty) {
//      val pivots: String = sparkSqlCondition.getCompareList.parallelStream().collect(Collectors.joining("#"))
//      //      println(pivots)
//      //分组
//      if (!groupList.isEmpty && groupList.size() > 0) {
//        val firstGroup = groupList.get(0)
//        if (!groupList.isEmpty) {
//          df = df.repartition(1).groupBy(firstGroup, groupList.asScala.tail: _*).pivot(pivots).sum(sparkSqlCondition.getSumList.asScala: _*)
//        }
//        else {
//          df = df.repartition(1).groupBy(firstGroup).pivot(pivots).sum(sparkSqlCondition.getSumList.asScala: _*)
//        }
//      }
//    }
//    //    df.show(false)
//    parseResult(df, sparkSqlCondition)
//
//  }
//
//  //结果解析
//  def parseResult(result: DataFrame, sparkSqlCondition: SparkSqlCondition): Unit = {
//    var groupList: util.List[String] = sparkSqlCondition.getGroupList
//    var compareList: util.List[String] = sparkSqlCondition.getCompareList
//    //结果标题
//    val arrayNames = result.columns
//    //结果数据
//    val rows = result.getClass.getMethod("take", classOf[Int])
//      .invoke(result, 1000: java.lang.Integer)
//      .asInstanceOf[Array[Row]]
//      .map(_.toSeq)
//
//    var map: Map[String, List[String]] = Map()
//    for (nameIndex <- arrayNames.indices) {
//      var stringBuilder = new StringBuilder
//      for (row <- rows.indices) {
//        val rowArray = rows.apply(row)
//        if (rowArray.apply(nameIndex) == null) {
//          stringBuilder ++= "null".concat(",")
//        }
//        else {
//          stringBuilder ++= rowArray.apply(nameIndex).toString.concat(",")
//        }
//      }
//      val list = stringBuilder.substring(0, stringBuilder.length - 1).split(",").toList
//      map += (arrayNames.apply(nameIndex) -> list)
//    }
//    var xAxisList: List[XAxis] = List()
//    var yAxisList: List[YAxis] = List()
//    map.keys.foreach(key => {
//      if (groupList.contains(key)) {
//        var xAxis: XAxis = new XAxis
//        xAxis.name = key
//        xAxis.data = map(key)
//        val temp = xAxis +: xAxisList
//        xAxisList = temp ::: xAxisList
//      }
//      else {
//        var yAxis: YAxis = new YAxis
//        yAxis.name = key
//        yAxis.data = map(key)
//        val temp = yAxis +: yAxisList
//        yAxisList = temp ::: yAxisList
//      }
//    })
//    var response = new ResponseResult
//
//    //    implicit def formats = DefaultFormats
//
//    response.setXAxis(xAxisList)
//    response.setYAxis(yAxisList)
//    val mapper = new ObjectMapper()
//    mapper.registerModule(DefaultScalaModule)
//    val out = new StringWriter
//    mapper.writeValue(out, response)
//    val json = out.toString
//
//
//  }
//
//
//}

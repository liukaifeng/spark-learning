package com.lkf.v3

import java.io.{BufferedReader, File, FileOutputStream, InputStreamReader}

import cn.com.tcsl.cmp.client.dto.report.condition.{DateUtils, SparkSqlCondition}
import org.apache.livy.client.common.ext.FreeMarkerUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.json4s.{DefaultFormats, JObject}
import org.slf4j.LoggerFactory

/**
  * 数据预测及预测结果处理
  *
  * @author 刘凯峰
  *         2019-07-24 15-49
  */
object DataPredictUtil {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private implicit def formats = DefaultFormats

  def main(args: Array[String]): Unit = {
    val url:String="jdbc:hive2://192.168.12.204:21050/;auth=noSasl"
//    println(url.indexOf("//"))
//    println(url.indexOf("/;"))
    println(url.substring(url.indexOf("//")+2,url.indexOf("/;")))
//    val jsonStr = "{\"weather\": [{\"date\": \"2018-12-17\", \"sum_recv_money\": \"109264.0\", \"m_weather\": \"1\", \"n_weather\": \"1\", \"m_temper\": \"8\", \"n_temper\": \"-4\", \"m_wind\": \"1\"}, {\"date\": \"2018-12-18\", \"sum_recv_money\": \"114565.0\", \"m_weather\": \"1\", \"n_weather\": \"1\", \"m_temper\": \"9\", \"n_temper\": \"-1\", \"m_wind\": \"3\"}, {\"date\": \"2018-12-19\", \"sum_recv_money\": \"118283.0\", \"m_weather\": \"1\", \"n_weather\": \"3\", \"m_temper\": \"9\", \"n_temper\": \"-2\", \"m_wind\": \"3\"}, {\"date\": \"2018-12-20\", \"sum_recv_money\": \"122430.0\", \"m_weather\": \"2\", \"n_weather\": \"2\", \"m_temper\": \"7\", \"n_temper\": \"-4\", \"m_wind\": \"1\"}, {\"date\": \"2018-12-21\", \"sum_recv_money\": \"141592.0\", \"m_weather\": \"1\", \"n_weather\": \"3\", \"m_temper\": \"7\", \"n_temper\": \"-4\", \"m_wind\": \"1\"}, {\"date\": \"2018-12-22\", \"sum_recv_money\": \"154409.0\", \"m_weather\": \"3\", \"n_weather\": \"1\", \"m_temper\": \"6\", \"n_temper\": \"-6\", \"m_wind\": \"1\"}, {\"date\": \"2018-12-23\", \"sum_recv_money\": \"179729.0\", \"m_weather\": \"1\", \"n_weather\": \"1\", \"m_temper\": \"2\", \"n_temper\": \"-10\", \"m_wind\": \"5\"}, {\"date\": \"2018-12-24\", \"sum_recv_money\": \"152522.0\", \"m_weather\": \"1\", \"n_weather\": \"1\", \"m_temper\": \"3\", \"n_temper\": \"-6\", \"m_wind\": \"1\"}, {\"date\": \"2018-12-25\", \"sum_recv_money\": \"152237.0\", \"m_weather\": \"3\", \"n_weather\": \"1\", \"m_temper\": \"2\", \"n_temper\": \"-6\", \"m_wind\": \"3\"}, {\"date\": \"2018-12-26\", \"sum_recv_money\": \"119577.0\", \"m_weather\": \"3\", \"n_weather\": \"3\", \"m_temper\": \"-3\", \"n_temper\": \"-12\", \"m_wind\": \"3\"}, {\"date\": \"2018-12-27\", \"sum_recv_money\": \"104848.0\", \"m_weather\": \"3\", \"n_weather\": \"1\", \"m_temper\": \"-6\", \"n_temper\": \"-15\", \"m_wind\": \"4\"}, {\"date\": \"2018-12-28\", \"sum_recv_money\": \"133125.0\", \"m_weather\": \"1\", \"n_weather\": \"1\", \"m_temper\": \"-4\", \"n_temper\": \"-12\", \"m_wind\": \"4\"}, {\"date\": \"2018-12-29\", \"sum_recv_money\": \"175954.0\", \"m_weather\": \"1\", \"n_weather\": \"1\", \"m_temper\": \"-3\", \"n_temper\": \"-12\", \"m_wind\": \"4\"}, {\"date\": \"2018-12-30\", \"sum_recv_money\": \"207739.0\", \"m_weather\": \"1\", \"n_weather\": \"3\", \"m_temper\": \"0\", \"n_temper\": \"-8\", \"m_wind\": \"1\"}, {\"date\": \"2018-12-31\", \"sum_recv_money\": \"255887.0\", \"m_weather\": \"2\", \"n_weather\": \"2\", \"m_temper\": \"-2\", \"n_temper\": \"-9\", \"m_wind\": \"1\"}, {\"date\": \"2019-10-22\", \"sum_recv_money\": \"101572\", \"m_weather\": \"1\", \"n_weather\": \"3\", \"m_temper\": \"21\", \"n_temper\": \"10\", \"m_wind\": \"3\"}], \"feature_score\": {\"daytemperature\": \"102\", \"nighttemperature\": \"65\", \"weekend\": \"37\", \"festival\": \"26\", \"settlebizweek\": \"25\", \"daywindforce\": \"25\", \"month\": \"20\", \"dayweather\": \"20\", \"nightweather\": \"20\", \"season\": \"9\"}}"
//
//    val parseObj = parse(jsonStr)
//
//    //提取天气信息
//    val weather = compact(parseObj \ "weather")
//    //提取权重值
//    val feature_score = compact(parseObj \ "feature_score")
//    println(feature_score)
//    //根据预测结果json串创建数据集
//    val spark = SparkSession.builder().config(new SparkConf().setMaster("local[*]")).getOrCreate()
//    import spark.implicits._
//
//    //根据预测结果json串创建数据集
//    var ds = spark.createDataset(Seq(weather))
//    //根据JSON数据集生产dataframe
//    var df = spark.read.json(ds)
//    df.show()
  }

  //经营预测模型请求参数模板
  val predictParamFtl = "{'city':'${city}','group_code':'${group_code}','store_name':'${store_name}','history_date':'${history_date}','predict_days':${predict_days},'all_data':${all_data},'impala_host':'${impala_host}','impala_port':${impala_port},'weather_table':'${weather_table}','sales_table':'${sales_table}','holiday_table':'${holiday_table}'}"

  /**
    * 异步执行解析预测结果数据
    *
    * @param sparkSqlCondition 封装好的请求条件
    **/
  def asyncProcessPredictData(sparkSession: SparkSession, sparkSqlCondition: SparkSqlCondition) = {
    val map = getPredictConf(sparkSqlCondition)
    //    val f: Future[Boolean] = Future {
    //创建spark会话
    val spark: SparkSession = SparkSourceContext.getSparkSession(sparkSqlCondition)
    //根据请求条件转换为预测模型条件
    val param = FreeMarkerUtils.parseFtlContent(predictParamFtl, map)
    //调用预测模型，生成预测数据
    val predictResult = executePredict(spark, param)
    if (predictResult != null && predictResult.length > 0) {
      import spark.implicits._
      //解析预测结果
      val parseObj = parse(predictResult)
      //提取天气信息
      val weather = compact(parseObj \ "weather")
      //提取权重值
      val feature_score = compact(parseObj \ "feature_score")
      //根据预测结果json串创建数据集
      var ds = spark.createDataset(Seq(weather))
      //根据JSON数据集生产dataframe
      var df = spark.read.json(ds)
      df.show()
      //根据预测结果集创建临时视图
      df.createOrReplaceTempView("tb_predict")
      //将预测结果集转为返回值名称
      df = spark.sql("select date as ljc_group_x_settle_biz_datestr1561724067000_0,m_weather as ljc__x_nightweatherstr1563948387000_0,m_temper as ljc__x_nighttemperaturestr1563948392000_0,sum_recv_money as ljc__x_recv_moneystr1563948398000_0 from tb_predict")
      // 获取数据结构
      val jSchema = parse(df.schema.json)
      // 获取数据
      val jRows = ExtractionUtil.decompose(df.take(100).map(_.toSeq))
      // 拼接返回值
      val content: JObject = ("application/json" -> (("schema" -> jSchema) ~ ("data" -> jRows) ~ ("total" -> 0) ~ ("attachment" -> feature_score)))
      //拼接缓存对象
      val cacheContent = ("status" -> "OK") ~ ("execution_count" -> 0) ~ ("data" -> content)
      //预测结果缓存对象
      val predictResultCache = new PredictResultCacheBean(sparkSqlCondition.getTracId, compact(render(cacheContent)))
      //将预测结果保存到MongoDB
      InterpreterUtil.saveSqlExtLog(predictResultCache, sparkSqlCondition, SqlExecutionEnum.PREDICTION_DATA.toString)
    } else {
      //拼接缓存对象
      val cacheContent = ("status" -> "ERROR") ~ ("execution_count" -> 0) ~ ("data" -> null)
      //预测结果缓存对象
      val predictResultCache = new PredictResultCacheBean(sparkSqlCondition.getTracId, compact(render(cacheContent)))
      //将预测结果保存到MongoDB
      InterpreterUtil.saveSqlExtLog(predictResultCache, sparkSqlCondition, SqlExecutionEnum.PREDICTION_DATA.toString)
    }
    //      true
    //    }
    //    f onComplete {
    //      case Success(_) => logger.info("==========经营预测执行完成===========")
    //      case Failure(t) => logger.error("经营预测出现异常: " + t.getMessage)
    //    }
  }

  def getPredictConf(sparkSqlCondition: SparkSqlCondition): java.util.Map[String,Object] ={
    val map = sparkSqlCondition.getPredict2PythonDTO
    val url= HiveJdbcPoolUtil.getUrl(sparkSqlCondition.getMongoConfigMap)
    val hostPort=url.substring(url.indexOf("//")+2,url.indexOf("/;"))
    val host=hostPort.split(":")(0)
    val port=hostPort.split(":")(1)
    map.put("impala_host", host)
    map.put("impala_port", port)
    map.put("weather_table", HiveJdbcPoolUtil.confMap.getProperty("weatherTable"))
    map.put("sales_table", HiveJdbcPoolUtil.confMap.getProperty("salesTable"))
    map.put("holiday_table", HiveJdbcPoolUtil.confMap.getProperty("holidayTable"))
    map
  }

  /**
    * 调用预测模型，执行预测算法，得到预测结果
    *
    * @param spark          spark会话
    * @param predictPyParam 预测模型执行参数，json字符串的格式
    */
  def executePredict(spark: SparkSession, predictPyParam: String): String = {
    logger.info("经营预测模型请求参数：{}", predictPyParam)
    //预测模型路径
    val predictPyPath = getClass.getResource("/xgboost_v8.py").getPath.substring(1)
    //调用预测模型命令
    val runtimeExecArray = Array[String]("python", predictPyPath, predictPyParam)
    //用预测模型
    val process = Runtime.getRuntime.exec(runtimeExecArray)
    //获取输入流对象
    val inputStream = process.getInputStream
    //根据输入流对象实例化字符读取对象
    val bufferReader = new BufferedReader(new InputStreamReader(inputStream, "utf-8"))
    //读取预测结果
    val predictResult = bufferReader.readLine()
    //等待结果读取完成
    process.waitFor
    logger.info("预测结果数据：{}", predictResult)
    //返回预测结果
    predictResult
  }

  def getXgboostPath: String = {
    val predictPythonScript = "/xgboost_v2.py"
    //获取脚本InputStream
    val in = this.getClass.getResourceAsStream(predictPythonScript)
    var localFile = ""
    //获取jar所在的集群路径
    val jarPath = this.getClass.getProtectionDomain.getCodeSource.getLocation.getPath.replace("\\", "/")
    val pyDir = jarPath.substring(0, jarPath.lastIndexOf("/")) + predictPythonScript
    if (in != null) {
      val f = new File(pyDir)
      if (!f.exists()) f.mkdirs
      localFile = pyDir + predictPythonScript
      val out = new FileOutputStream(localFile)
      val buf = new Array[Byte](1024)
      try {
        var nLen = in.read(buf)
        while (nLen != -1) {
          out.write(buf, 0, nLen)
          nLen = in.read(buf)
        }
      } catch {
        case e: Exception => logger.error(e.getMessage)
        case _ => logger.error("Read CompressFile.py Exception")
      } finally {
        in.close()
        out.close()
      }
    } else {
      logger.error("a NULL error occurred when Read CompressFile.py in jar,maybe the path is invalid!")
    }
    localFile
  }


  /**
    * 生产预测数据，测试使用
    **/
  def producePredictData(spark: SparkSession): DataFrame = {
    import spark.implicits._
    val df = Seq(
      ("2019-07-20", java.math.BigDecimal.valueOf(1260.35), "晴天", 30),
      ("2019-07-22", java.math.BigDecimal.valueOf(100.35), "阴天", 36),
      ("2019-07-23", java.math.BigDecimal.valueOf(658.35), "小雨", 12),
      ("2019-07-24", java.math.BigDecimal.valueOf(9848.35), "中雨", 35)
    ).toDF("settle_biz_date", "amount", "weather", "temperature")
    df
  }
}


/**
  * 预测结果缓存对象
  **/
class PredictResultCacheBean(var tracId: String, var predictResult: String, var beginTime: String = DateUtils.getCurrentTime) {}
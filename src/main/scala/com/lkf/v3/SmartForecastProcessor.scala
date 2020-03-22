package com.lkf.v3

import java.io._
import java.util
import java.util.stream.Collectors

import cn.com.tcsl.cmp.client.dto.report.condition.{DateUtils, SparkSqlCondition}
import com.google.gson.Gson
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
 * 智能预货预测模型处理器
 *
 * @author kaifeng
 * @date 2020-03-10 9:28
 */
class SmartForecastProcessor {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private implicit def formats = DefaultFormats

  import scala.concurrent.ExecutionContext.Implicits.global

  //预测模型python脚本地址
  private var predictPyPath: String = _
  //预测模型入参
  private var smartForecastConfMap: util.Map[String, AnyRef] = _
  //天气预报表
  private val WEATHER_TABLE = "weatherTable"
  //节假日表
  private val HOLIDAY_TABLE = "holidayTable"
  //预测脚本在hdfs中的路径
  private val PREDICT_SCRIPT_PATH = "script_path"
  //预测脚本名称
  private val PREDICT_SCRIPT_NAME = "script_name"
  //hdfs 地址
  private val HDFS_URL = "hdfsUrl"

  /**
   * 异步调用预测脚本
   *
   * @param sparkSqlCondition 封装好的请求条件
   **/
  def asyncProcessSmartForecast(sparkSession: SparkSession, sparkSqlCondition: SparkSqlCondition) = {
//    val f: Future[Boolean] = Future {
      //预测请求参数
      smartForecastConfMap = parsingPredictConf(sparkSqlCondition)
      //创建spark会话
//      val spark: SparkSession = SparkSourceContext.getSparkSession(sparkSession, sparkSqlCondition)
      val spark: SparkSession = sparkSession
      val gson: Gson = new Gson()
      //根据请求条件转换为预测模型条件
      val param = gson.toJson(smartForecastConfMap)
      //调用预测模型，生成预测数据
      val predictResult = executeSmartForecast(spark, param)
      true
//    }
//    f onComplete {
//      case Success(_) => logger.info("==========智能预货请求成功===========")
//      case Failure(t) =>
//        logger.error("智能预货请求异常: " + t.getMessage)
//    }
  }

  /**
   * 解析智能预货参数
   **/
  def parsingPredictConf(sparkSqlCondition: SparkSqlCondition): java.util.Map[String, Object] = {
    val map = sparkSqlCondition.getSmartForecastCondition
    //获取impala连接地址
    val url = HiveJdbcPoolUtil.getUrl(sparkSqlCondition.getMongoConfigMap)
    //从impala连接地址解析出host和端口号
    val hostPort = url.substring(url.indexOf("//") + 2, url.indexOf("/;"))
    //host
    val host = hostPort.split(":")(0)
    //端口号
    val port = hostPort.split(":")(1)
    map.put("forecast_code", sparkSqlCondition.getTracId)
    map.put("impala_host", host)
    map.put("impala_port", port)
    map.put("weather_table", HiveJdbcPoolUtil.confMap.getProperty(WEATHER_TABLE))
    map.put("holiday_table", HiveJdbcPoolUtil.confMap.getProperty(HOLIDAY_TABLE))
    map
  }


  /**
   * 调用预测模型，执行预测算法，得到预测结果
   *
   * @param spark          spark会话
   * @param predictPyParam 预测模型执行参数，json字符串的格式
   */
  def executeSmartForecast(spark: SparkSession, predictPyParam: String): String = {
    logger.info("智能预货请求参数：{}", predictPyParam)
    //智能预货模型路径
    if (predictPyPath == null || predictPyPath.isEmpty) {
      predictPyPath = getXgboostPath
    }
    logger.info(s"predict model script path：{}", predictPyPath)
    val pythonPath = predictPyPath.substring(0, predictPyPath.lastIndexOf("/") + 1).concat("environment/bi_predict_python/bin/python3 ")
    //调用预测模型命令
    val runtimeExecArray = pythonPath + " " + predictPyPath + " " + predictPyParam
    logger.info(s"=================$runtimeExecArray")
    //用预测模型
    var process = Runtime.getRuntime.exec(runtimeExecArray)
    logger.info(s"=================process.isAlive:${process.isAlive}")
    var inputStream: InputStream = null
    var bufferReader: BufferedReader = null
    var predictResult: String = null
    try {
      //获取输入流对象
      inputStream = process.getInputStream
      //根据输入流对象实例化字符读取对象
      bufferReader = new BufferedReader(new InputStreamReader(inputStream, "utf-8"))
      //读取返回结果
      predictResult = bufferReader.readLine()
      //输出错误流
      val out = consumeInputStream(process.getErrorStream)
      logger.info(s"process ErrorStream：$out")
      //等待结果读取完成
      val exitValue = process.waitFor
      logger.info(s"process exitValue：$exitValue")
      logger.info(s"predict data：$predictResult")
    } catch {
      case e: Exception => logger.error(e.getMessage)
      case _ => logger.error("Read xgboost.py Exception")
    } finally {
      inputStream.close()
      bufferReader.close()
      process.destroy()
      inputStream = null
      bufferReader = null
      process = null
    }
    //返回预测结果
    predictResult
  }

  /**
   * 将智能预货模型脚本读取到当前container
   **/
  def getXgboostPath: String = {
    //hdfs 地址
    val hdfsUrl = HiveJdbcPoolUtil.confMap.getProperty(HDFS_URL)
    //预测脚本在hdfs中的路径
    val predictScriptPath = smartForecastConfMap.get(PREDICT_SCRIPT_PATH)
    //预测脚本文件
    val predictScriptFile = "/".concat(smartForecastConfMap.get(PREDICT_SCRIPT_NAME).toString)
    //获取脚本InputStream
    val in = HdfsUtil.readHDFSFile(hdfsUrl, predictScriptPath + predictScriptFile)
    //获取jar所在的集群路径
    val jarPath = this.getClass.getProtectionDomain.getCodeSource.getLocation.getPath.replace("\\", "/")
    logger.info(s"===========jarPath========$jarPath")
    //截取jar路径
    val pyDir = jarPath.substring(0, jarPath.lastIndexOf("/"))
    logger.info(s"===========pyDir=====$pyDir")
    //读取到当前container的完整路径
    var localFile = ""
    if (in != null) {
      val f = new File(pyDir)
      if (!f.exists()) f.mkdirs
      localFile = pyDir + predictScriptFile
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
        case _ => logger.error("Read xgboost.py Exception")
      } finally {
        in.close()
        out.close()
      }
    } else {
      logger.error("a NULL error occurred when Read xgboost.py in jar,maybe the path is invalid!")
    }
    localFile
  }

  /**
   * 读取脚本的输出信息
   **/
  def consumeInputStream(inputstream: InputStream): String = {
    val br: BufferedReader = new BufferedReader(new InputStreamReader(inputstream, "utf-8"))
    br.lines().collect(Collectors.joining("#"))
  }
}


/**
 * 预测请求对象
 **/
class PredictRequestBean(var tracId: String, var requestParam: String, var errorMsg: String, var beginTime: String = DateUtils.getCurrentTime) {}


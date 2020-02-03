package com.lkf.rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging

/**
  * todo 一句话描述该类的用途
  *
  * @author 刘凯峰
  * @date 2019-07-09 17-07
  */
object SparkUtils extends Logging {
  /**
    * 执行作业
    */
  def runJob(job: (Array[String], SparkContext) => Unit,
             args: Array[String],
             context: SparkContext,
             errorMsg: String = "任务执行失败"): Unit = {
    val startTime = System.currentTimeMillis()
    try {
      job(args, context)
    } catch {
      case ex: Throwable => ex.printStackTrace(); throw ex
    } finally {
      log.info("Cost Time: %s".format(System.currentTimeMillis() - startTime))
    }
  }
  /**
    * 初始化spark上下文
    */
  def initSparkContext(appName: String, master: String = null): SparkContext = {
    val sparkConf = new SparkConf().setAppName(appName)
    sparkConf.set("spark.files.userClassPathFirst", "true")
    if (null == sparkConf.get("spark.master", null) && null == master)
      sparkConf.set("spark.master", "local[*]")
    if (null != master) sparkConf.set("spark.master", master)
    new SparkContext(sparkConf)
  }
}

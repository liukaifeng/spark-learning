package com.lkf.v3

import java.io.StringWriter

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.SparkConf
import org.apache.spark.scheduler._
import org.slf4j.LoggerFactory

class MySparkAppListener(val sparkConf: SparkConf) extends SparkListener {

  private val logger = LoggerFactory.getLogger(this.getClass)

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    logger.info("onApplicationStart =========================")
    val map = Map("appId" -> applicationStart.appId,
      "appName" -> applicationStart.appName,
      "sparkUser" -> applicationStart.sparkUser
    )
    logger.info(objectToJson(map))
    logger.info("onApplicationStart =========================")
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    logger.info("onApplicationEnd appId:{}", applicationEnd.time)
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val begin = System.currentTimeMillis()
    logger.info("onJobStart =========================")
    logger.info("onJobStart, jobId:{},stageIds:{}", jobStart.jobId, jobStart.stageIds)
    logger.info(objectToJson(jobStart.properties))
    val end = System.currentTimeMillis()
    logger.info("onJobStart ==========={}==============", end - begin)

  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    logger.info("onJobEnd, jobId:{},stageIds:{}", jobEnd.jobId, jobEnd.jobResult)
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    logger.info("onTaskStart,taskInfo:{}", taskStart.taskInfo)
  }

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = {
    logger.info("onTaskGettingResult, taskInfo:", taskGettingResult.taskInfo)

  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    logger.info("onTaskEnd, stageId:", taskEnd.stageId)
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
}

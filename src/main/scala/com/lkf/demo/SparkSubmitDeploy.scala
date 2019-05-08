//package com.lkf.demo
//
//import java.util
//import java.util.Map
//
//import org.apache.spark.SparkConf
//import org.apache.spark.deploy.rest.{CreateSubmissionRequest, RestSubmissionClient, SubmitRestProtocolResponse}
//
//
//object SparkSubmitDeploy {
//  def submit(): Unit = {
//    val appResource = "hdfs://192.168.0.181:8020/opt/guoxiang/wordcount.jar"
//    val mainClass = "com.fly.spark.WordCount"
//    val args = Array("hdfs://192.168.0.181:8020/opt/guoxiang/wordcount.txt", "hdfs://192.168.0.181:8020/opt/guoxiang/wordcount")
//    val sparkConf = new SparkConf
//    // 下面的是参考任务实时提交的Debug信息编写的
//    sparkConf.setMaster("spark://192.168.0.181:6066").setAppName("carabon" + " " + System.currentTimeMillis).set("spark.executor.cores", "4").set("spark.submit.deployMode", "cluster").set("spark.jars", appResource).set("spark.executor.memory", "1g").set("spark.cores.max", "4").set("spark.driver.supervise", "false")
//    val env = System.getenv
//    var response = null
//    try {
//      val restSubmissionClient = new RestSubmissionClient("")
//      val createSubmissionRequest = restSubmissionClient.constructSubmitRequest(appResource, mainClass, args, sparkConf, null)
//      response = restSubmissionClient.createSubmission(createSubmissionRequest)
//    } catch {
//      case e: Exception =>
//        e.printStackTrace()
//    }
//    System.out.println(response.toJson)
//  }
//}

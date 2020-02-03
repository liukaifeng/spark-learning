package com.lkf.pmml

import java.io.{BufferedReader, InputStreamReader}

/**
  * todo 一句话描述该类的用途
  *
  * @author 刘凯峰
  * @date 2019-07-25 20-25
  */
object InvokeByRuntimeScala {
  def main(args: Array[String]): Unit = {
    val param = "{'city': '重庆', 'group_code': '9759', 'store_code': '11748,11755', 'history_date': '2017-11-09,2018-10-15'}"
    //    val param = "{\"predictDimension\":[\"settle_biz_date\"],\"predictFilter\":{\"historyDate\":[\"1561910400000\",\"1563206400000\"],\"city\":[\"天津\"],\"storeCode\":[\"3621\",\"5379\"],\"predictionDate\":[\"7\"]},\"predictIndex\":[\"nightweather\",\"nighttemperature\",\"recv_money\"]}"
    executePredict(param)
  }

  def executePredict(predictPyParam: String): String = {
    //预测模型路径
    val predictPyPath = "G:\\workspace\\python3\\xgboost_test(3).py"
    //调用预测模型命令 {'city': '重庆', 'group_code': '9759', 'store_code': '11750,11752', 'history_date': '2018-10-01,2018-10-15'}
    val runtimeExecArray = Array[String]("python", predictPyPath, predictPyParam)
    //用预测模型
    val process = Runtime.getRuntime.exec(runtimeExecArray)
    //获取输入流对象
    val inputStream = process.getInputStream
    //根据输入流对象实例化字符读取对象
    val bufferReader = new BufferedReader(new InputStreamReader(inputStream, "GBK"))
    //读取预测结果
    val predictResult = bufferReader.readLine()
    //等待结果读取完成
    process.waitFor
    println(predictResult)
    //返回预测结果
    predictResult
  }
}

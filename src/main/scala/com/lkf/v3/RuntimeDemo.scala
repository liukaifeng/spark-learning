package com.lkf.v3

import java.io.{BufferedReader, InputStreamReader}

/**
  * todo 一句话描述该类的用途
  *
  * @author 刘凯峰
  * @date 2019-07-24 13-56
  */
object RuntimeDemo {
  def main(args: Array[String]): Unit = {
    val exe = "python"
    val command = "G:\\workspace\\python3\\hello.py"
    val num1 = "1"
    val num2 = "2"
    val cmdArr = Array[String](exe, command, num1, num2)
    val process = Runtime.getRuntime.exec(cmdArr)
    val inputStream = process.getInputStream
    val bufferReader = new BufferedReader(new InputStreamReader(inputStream))
    val str = bufferReader.readLine()
    process.waitFor
    println(str)
  }
}

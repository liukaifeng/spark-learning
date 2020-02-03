package com.lkf.v1


object SparkDemo {
  def main(args: Array[String]): Unit = {

    val clazz = Class.forName("com.lkf.v1.Person")
    val result = clazz //使用该对象去获取私有函数
      .getDeclaredMethod(s"getName") //并得到该函数入参的数据类型,如有多个入参,要声明多个classOf
      .invoke(clazz.newInstance()) //激活该函数,传入入参
      .asInstanceOf[String] //最后结果强转下类型,scala默认是返回AnyRef类型
    println(result.toString)
  }
}

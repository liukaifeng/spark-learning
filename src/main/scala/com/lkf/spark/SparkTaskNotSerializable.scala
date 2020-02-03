package com.lkf.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * todo 一句话描述该类的用途
  *
  * @author 刘凯峰
  * @date 2019-10-15 16-59
  */
object SparkTaskNotSerializable {
  //  def main(args: Array[String]): Unit = {
  //
  //    //    问题
  //        val conf = new SparkConf().setMaster("local[*]").setAppName("test")
  //        val sc = new SparkContext(conf)
  //        val rdd = sc.parallelize(1 to 50, 5)
  //        val usz = new UnserializableClass()
  //        rdd.map(x=>usz.method(x)).foreach(println(_))
  //    //    解法1
  ////    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
  ////    val sc = new SparkContext(conf)
  ////    val rdd = sc.parallelize(1 to 50, 5)
  ////    // 在 map 中实例化对象 UnserializableClass
  ////    rdd.map(x => new UnserializableClass().method(x)).foreach(println(_))
  //  }


  //  def main(args: Array[String]): Unit = {
  //    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
  //    val sc = new SparkContext(conf)
  //    val rdd = sc.parallelize(1 to 50, 5)
  //    val usz = new UnserializableClass()
  //    rdd.map(usz.method).foreach(println(_))
  //  }


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
    //指定序列化类为KryoSerializer
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //将UnserializableClass注册到kryo需要序列化的类中
    conf.registerKryoClasses(Array(classOf[com.lkf.spark.UnserializableClass]))
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(1 to 50, 5)
    val usz = new UnserializableClass()
    rdd.map(x => usz.method(x)).foreach(println(_))
  }
}


class UnserializableClass {
  def method(x: Int): Int = {
    x * 2
  }
}

//class UnserializableClass extends java.io.Serializable {
//  def method(x: Int): Int = {
//    x * 2
//  }
//}

//class UnserializableClass {
//  //method方法
//  /*def method(x:Int):Int={
//    x * 2
//  }*/
//
//  //method函数
//  val method = (x:Int)=>x*2
//}
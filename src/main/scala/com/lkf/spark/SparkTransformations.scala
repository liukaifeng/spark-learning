package com.lkf.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * todo 一句话描述该类的用途
  *
  * @author 刘凯峰
  * @date 2019-10-16 15-14
  */
object SparkTransformations {
  val sparkConf: SparkConf = new SparkConf().setAppName("SparkTransformations").setMaster("local[*]")
  val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  def main(args: Array[String]): Unit = {

    //mapTranformation(sparkSession.sparkContext)
//    flatMapTranformation(sparkSession.sparkContext)
    mapPartitionsWithIndex(sparkSession.sparkContext)
  }

  /**
    * 算子操作 map
    *
    * @param sc
    */
  def mapTranformation(sc: SparkContext): Unit = {
    val nums = sc.parallelize(1 to 10)
    val map = nums.map(x => x * 2)
    println("原数据：" + nums.collect.toList)
    println("map后的数据：" + map.collect.toList)
  }

  /**
    * flatMap
    *
    * @param sc
    */
  def flatMapTranformation(sc: SparkContext): Unit = {
    val arrays = Array("热烈 庆祝", "中华人民 共和国", "成立70 周年")
    val data = sc.parallelize(arrays)
    val flatmapData = data.flatMap(x => x.split(" "))
    println("原数据：" + data.collect.toList)
    println("flatmap 结果：" + flatmapData.collect.toList)
  }

  def mapPartitionsWithIndex(sc: SparkContext): Unit = {
    val data = Array(Tuple2(100, "Spark"), Tuple2(200, "Hadoop"), Tuple2(100, "Scala"), Tuple2(120, "Hbase"))
    val dataRDD = sc.parallelize(data)

   val newData= dataRDD.mapPartitionsWithIndex((index, iterator) => {
      val list = iterator.toList
      list.map(x => x + "->" + index).iterator
    })
    println(newData.collect().toList)
  }

  def myfunc(index: Int, iter: Iterator[String]): Iterator[String] = {
    iter.map(x => index + "," + x)
  }

  /**
    * 算子操作 filter
    *
    * @param sc
    */
  def filterTranformation(sc: SparkContext): Unit = {
    println("算子操作 filter")
    val nums = sc.parallelize(1 to 10) //根据集合创建RDD
    val filter = nums.filter(x => x % 2 == 0)
    filter.collect.foreach(println)
  }

  /**
    * groupByKey 相同的 key 进行分组
    *
    * @param sc
    */
  def groupByKeyTranformation(sc: SparkContext): Unit = {
    println("算子操作 groupByKey")

    val data = Array(Tuple2(100, "Spark"), Tuple2(200, "Hadoop"), Tuple2(100, "Scala"), Tuple2(120, "Hbase"))
    val dataRDD = sc.parallelize(data)
    val grouped = dataRDD.groupByKey()
    grouped.collect().foreach(println)
  }

  /**
    * reduceByKey  相同的 key 进行值累加
    *
    * @param sc
    */
  def reduceByKey(sc: SparkContext): Unit = {
    println("算子操作 reduceByKey")
    val path = "hdfs://s0:9000/library/wordcount/input/Data"
    val lines = sc.textFile(path) //读取文 件，并设置为一个Patitions (相当于几个Task去执行)

    val mapArray = lines.flatMap { x => x.split(" ") } //对每一行的字符串进行单词拆分并把把有行的拆分结果通过flat合并成为一个结果
    val mapMap = mapArray.map { x => (x, 1) }

    val result = mapMap.reduceByKey(_ + _) //对相同的Key进行累加
    result.collect().foreach(println)
  }

  /**
    * join 相同的 key 的值进行再次 key ,value 操作
    *
    * @param sc
    */
  def joinTRanformation(sc: SparkContext): Unit = {
    println("算子操作 join")
    val studentNames = Array(Tuple2(1, "Spark"), Tuple2(2, "Hadoop"), Tuple2(3, "Tachyon"))
    val studentScores = Array(Tuple2(1, 65), Tuple2(2, 70), Tuple2(3, 60))
    val names = sc.parallelize(studentNames)
    val scores = sc.parallelize(studentScores)
    val studentNameAndScore = names.join(scores)
    studentNameAndScore.collect().foreach(println)
  }

  /*
  * cogroup
  * @param sc
  */
  def cogroupTranformation(sc: SparkContext): Unit = {
    println("算子操作 cogroup")
    val studentNames = Array(Tuple2(1, "Spark"), Tuple2(2, "Hadoop"), Tuple2(3, "Tachyon"))
    val studentScores = Array(Tuple2(1, 65), Tuple2(2, 70), Tuple2(3, 60), Tuple2(4, 60), Tuple2(3, 65))
    val names = sc.parallelize(studentNames)
    val scores = sc.parallelize(studentScores)
    val studentNameAndScore = names.cogroup(scores)
    studentNameAndScore.collect().foreach(println)

  }


}

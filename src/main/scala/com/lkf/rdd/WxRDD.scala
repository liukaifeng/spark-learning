package com.lkf.rdd

import java.util.concurrent.TimeUnit

import com.lkf.druid.JDBCPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * todo 一句话描述该类的用途
  *
  * @author 刘凯峰
  * @date 2019-07-09 15-51
  */
//class WxRDD[T: ClassTag](sc: SparkContext,
//                         classTag: Class[T]) extends RDD[T](sc, Nil) {
//  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
//
//  }
//
//  override protected def getPartitions: Array[Partition] = {
//    (0 to 1).map { i => new JDBCPartition(i) }.toArray
//  }
//
//
//
//}
//
//case class WxPartition(idx: Int) extends Partition {
//  override def index: Int = idx
//}

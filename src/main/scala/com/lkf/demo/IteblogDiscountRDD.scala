package com.lkf.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

/**
  * todo 一句话描述该类的用途
  *
  * @author 刘凯峰
  *
  */
//class IteblogDiscountRDD(prev: RDD[SalesRecord], xxxxx: Double) extends RDD[SalesRecord](prev) {
//
//  //继承compute方法
//  override def compute(split: Partition, context: TaskContext): Iterator[SalesRecord] = {
//    firstParent[SalesRecord].iterator(split, context).map(salesRecord => {
//      val discount = salesRecord.itemValue * discountPercentage
//      new SalesRecord(salesRecord.id,
//        salesRecord.customerId, salesRecord.itemId, discount)
//    })
//  }
//
//  //继承getPartitions方法
//  override protected def getPartitions: Array[Partition] =
//    firstParent[SalesRecord].partitions
//}

package com.lkf.rdd

import org.apache.spark.sql.{Dataset, Row}

/**
  * todo 一句话描述该类的用途
  *
  * @author 刘凯峰
  * @date 2019-07-11 13-34
  */
class SparkRddTaskInfo {
  def getTask(dataSet: Dataset[Row]) {
    val size = dataSet.rdd.partitions.length
    println(s"==> partition size: $size " )
    import scala.collection.Iterator
    val showElements = (it: Iterator[Row]) => {
      val ns = it.toSeq
      import org.apache.spark.TaskContext
      val pid = TaskContext.get.partitionId
      println(s"[partition: $pid][size: ${ns.size}] ${ns.mkString(" ")}")
    }
    dataSet.foreachPartition(showElements)
  }
}

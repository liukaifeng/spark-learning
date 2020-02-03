package com.lkf.rdd

import java.util.Properties

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
  * todo 一句话描述该类的用途
  *
  * @author 刘凯峰
  * @date 2019-07-09 13-06
  */
class MultiTableRDD[T: ClassTag](sqlUrl: String,
                                 sqlProp: Properties,
                                 sql: String,
                                 tableNumber: Int,
                                 sc: SparkContext,
                                 classTag: Class[T]) extends RDD[T](sc, Nil) {


  if (tableNumber == 1) {
    //单表查询
    assert(!sql.contains("{index}"), "one table do not need table index")
  } else {
    assert(sql.contains("{index}"), "more than one  table  need table index")
  }


  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val map = Jdbc2BeanUtil.getSchemaMap(classTag)
//    val pool = new DBPool(sqlUrl, sqlProp)
    val listBuffer = ListBuffer[T]()
//    DBPool.withConnectQuery(pool, statement => {
//      val partiionedSql = sql.replace("{index}", split.index.toString)
//      val resultSet = statement.executeQuery(partiionedSql)
//      while (resultSet.next()) {
//        val result = Jdbc2BeanUtil.composeResult[T](map, resultSet, classTag)
//        listBuffer.+=(result)
//      }
//    })
    listBuffer.iterator
  }

  override protected def getPartitions: Array[Partition] = {
    Range(0, tableNumber).map(index => MysqlPartition(index)).toArray
  }

}

case class MysqlPartition(tableIndex: Int) extends Partition {
  override def index: Int = tableIndex
}

package com.lkf.druid

import java.sql.{Connection, ResultSet}

import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, Partitioner, SparkContext, TaskContext}

import scala.reflect.ClassTag


class JDBCPartition(idx: Int) extends Partition {
  override def index: Int = idx
}


class defineSparkPartition(numParts: Int) extends Partitioner {

  /** * 这个方法需要返回你想要创建分区的个数 */
  override def numPartitions: Int = numParts

  /** 这个函数需要对输入的key做计算，然后返回该key的分区ID，范围一定是0到numPartitions-1；
    *
    * @param key
    * @return
    **/
  override def getPartition(key: Any): Int = {
    var v = key.hashCode() % numPartitions
    println(v)
    v
  }

  /** 这个是Java标准的判断相等的函数，之所以要求用户实现这个函数是因为Spark内部会比较两个RDD的分区是否一样。
    *
    * @param other
    * @return
    **/
  override def equals(other: Any): Boolean = other match {
    case mypartition: defineSparkPartition =>
      mypartition.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions

}

/**
  * @param sc            spark 上下文
  * @param getConnection jdbc 连接对象
  * @param sql           完整sql语句
  * @param mapRow        结果集处理函数
  **/
class WxRDD[T: ClassTag](
                          sc: SparkContext,
                          getConnection: () => Connection,
                          sql: String,
                          mapRow: (ResultSet) => T = WxRDD.resultSetToObjectArray _)
  extends RDD[T](sc, Nil) with Logging {


  override def getPartitions: Array[Partition] = {
    (0 to 1).map { i => new JDBCPartition(i) }.toArray
  }

  override def compute(thePart: Partition, context: TaskContext): Iterator[T] = new NextIterator[T] {
    context.addTaskCompletionListener { context => closeIfNeeded() }
    val conn = getConnection()
    val stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    val rs = stmt.executeQuery()

    override def getNext(): T = {
      if (rs.next()) {
        mapRow(rs)
      } else {
        finished = true
        null.asInstanceOf[T]
      }
    }

    override def close() {
      try {
        if (null != rs) {
          rs.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing resultset", e)
      }
      try {
        if (null != stmt) {
          stmt.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing statement", e)
      }
      try {
        if (null != conn) {
          conn.close()
        }
        logInfo("closed connection")
      } catch {
        case e: Exception => logWarning("Exception closing connection", e)
      }
    }
  }
}

object WxRDD {
  /**
    * 结果集转换成Object 对象数组
    *
    * @param rs 结果集
    **/
  def resultSetToObjectArray(rs: ResultSet): Array[Object] = {
    Array.tabulate[Object](rs.getMetaData.getColumnCount)(i => rs.getObject(i + 1))
  }


}


/** 提供了一个基本的迭代器实现. */
private abstract class NextIterator[U] extends Iterator[U] {

  private var gotNext = false
  private var nextValue: U = _
  private var closed = false
  protected var finished = false

  /**
    * Method for subclasses to implement to provide the next element.
    *
    * If no next element is available, the subclass should set `finished`
    * to `true` and may return any value (it will be ignored).
    *
    * This convention is required because `null` may be a valid value,
    * and using `Option` seems like it might create unnecessary Some/None
    * instances, given some iterators might be called in a tight loop.
    *
    * @return U, or set 'finished' when done
    */
  protected def getNext(): U

  /**
    * Method for subclasses to implement when all elements have been successfully
    * iterated, and the iteration is done.
    *
    * <b>Note:</b> `NextIterator` cannot guarantee that `close` will be
    * called because it has no control over what happens when an exception
    * happens in the user code that is calling hasNext/next.
    *
    * Ideally you should have another try/catch, as in HadoopRDD, that
    * ensures any resources are closed should iteration fail.
    */
  protected def close()

  /**
    * Calls the subclass-defined close method, but only once.
    *
    * Usually calling `close` multiple times should be fine, but historically
    * there have been issues with some InputFormats throwing exceptions.
    */
  def closeIfNeeded() {
    if (!closed) {
      // Note: it's important that we set closed = true before calling close(), since setting it
      // afterwards would permit us to call close() multiple times if close() threw an exception.
      closed = true
      close()
    }
  }

  override def hasNext: Boolean = {
    if (!finished) {
      if (!gotNext) {
        nextValue = getNext()
        if (finished) {
          closeIfNeeded()
        }
        gotNext = true
      }
    }
    !finished
  }

  override def next(): U = {
    if (!hasNext) {
      throw new NoSuchElementException("End of stream")
    }
    gotNext = false
    nextValue
  }
}
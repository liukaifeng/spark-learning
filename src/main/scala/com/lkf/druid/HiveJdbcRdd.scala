package com.lkf.druid

import java.sql.{Connection, ResultSet}

import scala.reflect.ClassTag
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD


class JDBCPartition(idx: Int) extends Partition {
  override def index: Int = idx
}

class JDBCRDD[T: ClassTag](
                            sc: SparkContext,
                            getConnection: () => Connection,
                            sql: String,
                            mapRow: (ResultSet) => T = JDBCRDD.resultSetToObjectArray _)
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

object JDBCRDD {
  def resultSetToObjectArray(rs: ResultSet): Array[Object] = {
    Array.tabulate[Object](rs.getMetaData.getColumnCount)(i => rs.getObject(i + 1))
  }

  trait ConnectionFactory extends Serializable {
    @throws[Exception]
    def getConnection: Connection
  }

  def fakeClassTag[T]: ClassTag[T] = ClassTag.AnyRef.asInstanceOf[ClassTag[T]]

  def create[T](
                 sc: JavaSparkContext,
                 connectionFactory: ConnectionFactory,
                 sql: String,
                 mapRow: JFunction[ResultSet, T]): JavaRDD[T] = {

    val JDBCRDD = new JDBCRDD[T](
      sc.sc,
      () => connectionFactory.getConnection,
      sql,
      (resultSet: ResultSet) => mapRow.call(resultSet))(fakeClassTag)
    new JavaRDD[T](JDBCRDD)(fakeClassTag)
  }

  def create(
              sc: JavaSparkContext,
              connectionFactory: ConnectionFactory,
              sql: String
            ): JavaRDD[Array[Object]] = {

    val mapRow = new JFunction[ResultSet, Array[Object]] {
      override def call(resultSet: ResultSet): Array[Object] = {
        resultSetToObjectArray(resultSet)
      }
    }

    create(sc, connectionFactory, sql, mapRow)
  }
}


/** Provides a basic/boilerplate Iterator implementation. */
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
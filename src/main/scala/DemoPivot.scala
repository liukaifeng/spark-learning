//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.{Column, DataFrame, Row, SQLContext}
//import org.apache.spark.sql.functions.{lit, when}
//import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
//
//object DemoPivot {
//
//  def main(args: Array[String]) = {
//
//    def pivotColumn(df: DataFrame)(t: String): Column = {
//      val col = when(df("tag") <=> lit(t), df("value"))
//      col.alias(t)
//    }
//
//    def pivotFrame(sqlContext: SQLContext, df: DataFrame): DataFrame = {
//      val tags = df.select("tag").distinct.map(r => r.getString(0)).collect.toList
//      df.select(df("id") :: tags.map(pivotColumn(df)): _*)
//    }
//
//    def aggregateRows(value: Seq[Option[Any]], agg: Seq[Option[Any]]): Seq[Option[Any]] = {
//      for (i <- 0 until Math.max(value.size, agg.size)) yield i match {
//        case x if x > value.size => agg(x)
//        case y if y > agg.size => value(y)
//        case z if value(z).isEmpty => agg(z)
//        case a => value(a)
//      }
//    }
//
//    def collapseRows(sqlContext: SQLContext, df: DataFrame): DataFrame = {
//      // RDDs cannot have null elements, so pack into Options and unpack before returning
//      val rdd = df.map(row => (Some(row(0)), row.toSeq.tail.map(element => if (element == null) None else Some(element))))
//
//      val agg = rdd.reduceByKey(aggregateRows)
//      val aggRdd = agg.map{ case (key, list) => Row.fromSeq((key.get) :: (list.map(element => if (element.isDefined) element.get else null)).toList) }
//      sqlContext.createDataFrame(aggRdd, df.schema)
//    }
//
//    val conf = new SparkConf().setAppName("Simple Pivot Demo")
//    val sc = new SparkContext(conf)
//    val sqlContext = new SQLContext(sc)
//
//    val data = List((1, "US", "foo"), (1, "UK", "bar"), (1, "CA", "baz"),
//      (2, "US", "hoo"), (2, "UK", "hah"), (2, "CA", "wah"))
//    val rows = data.map(d => Row.fromSeq(d.productIterator.toList))
//    val fields = Array(StructField("id", IntegerType, nullable = false),
//      StructField("tag", StringType, nullable = false),
//      StructField("value", StringType, nullable = false))
//    val df = sqlContext.createDataFrame(sc.parallelize(rows), StructType(fields))
//    df.show()
//
//    val pivoted = pivotFrame(sqlContext, df)
//    pivoted.show()
//
//    val collapsed = collapseRows(sqlContext, pivoted)
//    collapsed.show()
//  }
//}
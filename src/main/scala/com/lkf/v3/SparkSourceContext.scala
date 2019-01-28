package com.lkf.v3


import org.apache.livy.client.ext.model.SparkSqlCondition
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

object SparkSourceContext {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  /*策略对象与标识之间的关系*/
  private val STRAGETY_CLASS_NAME = Map(0 -> "com.lkf.v3.SparkHdfsStrategy", 1 -> "com.lkf.v3.SparkHiveJdbcStrategy", 2 -> "com.lkf.v3.SparkKuduStrategy")
  /*抽象策略对象*/
  private var obj: ISessionStrategy = _

  /**
    * 根据类名和spark配置项实例化指定对象
    *
    * @param dataType 数据源类型
    * @param param    构造函数参数
    **/
  private[this] def getStrategyInstance(dataType: Int, param: SparkConf): ISessionStrategy = {
    try {
      //获取指定类名的Class对象
      val clazz = Class.forName(STRAGETY_CLASS_NAME.apply(dataType))
      //为构造函数赋值
      val const = clazz.getConstructor(classOf[SparkConf])
      //实例化对象
      obj = const.newInstance(param).asInstanceOf[ISessionStrategy]
    } catch {
      case e@(_: InstantiationException | _: IllegalAccessException | _: ClassNotFoundException | _: IllegalArgumentException | _: SecurityException) =>
        logger.error("反射实例化对象出现异常：{}", e.printStackTrace())
    }
    obj
  }

  def getSparkSession(sparkSqlCondition: SparkSqlCondition): SparkSession = {
    var sparkConf = new SparkConf()
      .set("spark.default.parallelism", "1")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.executor.instances", "1")
      .set("spark.driver.cores", "1")
      .set("spark.executor.cores", "1")
      .set("spark.extraListeners", "com.lkf.v3.MySparkAppListener")
      .setMaster("local[2]")
      .setAppName("spark_sql_default")

    //    var sparkConfMap: mutable.Map[String, String] = sparkSqlCondition.getSparkConfig.asScala
    //    if (sparkConfMap != null && sparkConfMap.nonEmpty) {
    //      sparkConfMap.keys.foreach(key => sparkConf.set(key, sparkConfMap(key)))
    //    }
    getStrategyInstance(sparkSqlCondition.getDataSourceType, sparkConf).sessionInit(sparkSqlCondition)
  }
}

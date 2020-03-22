/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.lkf.v3

import cn.com.tcsl.cmp.client.dto.report.condition.SparkSqlCondition
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

/**
  * spark 连接数据源上下文配置
  **/
object SparkSourceContext {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  /*策略对象与标识之间的关系*/
  private val STRAGETY_CLASS_NAME = Map(0 -> "org.apache.livy.repl.SparkHdfsStrategy", 1 -> "org.apache.livy.repl.SparkHiveJdbcStrategy", 2 -> "org.apache.livy.repl.SparkKuduStrategy")
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

  def getSparkSession(sparkSession: SparkSession, sparkSqlCondition: SparkSqlCondition): SparkSession = {
    logger.info("sparkConf:{}", sparkSession.sparkContext.getConf)
    getStrategyInstance(sparkSqlCondition.getDataSourceType, sparkSession.sparkContext.getConf).sessionInit(sparkSqlCondition)
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

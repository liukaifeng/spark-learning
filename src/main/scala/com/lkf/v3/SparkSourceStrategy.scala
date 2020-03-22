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
import org.apache.kudu.spark.kudu._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * session 初始化策略抽象接口
  */
trait ISessionStrategy {
  /**
    * 会话配置初始化
    *
    * @param sparkSqlCondition 配置项
    **/
  def sessionInit(sparkSqlCondition: SparkSqlCondition): SparkSession
}

/**
  * spark on hdfs
  */
class SparkHdfsStrategy(sparkConf: SparkConf) extends ISessionStrategy {

  /**
    * 会话配置初始化
    *
    * @param sparkSqlCondition 配置项
    **/
  override def sessionInit(sparkSqlCondition: SparkSqlCondition): SparkSession = {
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    sparkSession
  }
}

/**
  * spark on kudu
  */
class SparkKuduStrategy(sparkConf: SparkConf) extends ISessionStrategy {

  override def sessionInit(sparkSqlCondition: SparkSqlCondition): SparkSession = {
    //构造spark session
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    //kudu 数据表全名称
    val kuduTable = sparkSqlCondition.getKeyspace.concat(".").concat(sparkSqlCondition.getTable)

    val df: DataFrame = sparkSession
      .read
      .options(Map(
        "kudu.master" -> sparkSqlCondition.getKuduMaster,
        "kudu.table" -> kuduTable
      ))
      .kudu
      .filter(sparkSqlCondition.getCassandraFilter)
    val tempView = "temp_kudu_table_".concat(System.currentTimeMillis().toString)

    df.createOrReplaceTempView(tempView)
    val mainSql = sparkSqlCondition.getSelectSql
    val qoqSql = sparkSqlCondition.getSelectQoqSql

    sparkSqlCondition.setSelectSql(mainSql.replace(kuduTable, tempView))
    sparkSqlCondition.setSelectQoqSql(qoqSql.replace(kuduTable, tempView))
    sparkSession
  }
}


/**
  * spark hive jdbc 会话配置
  **/
class SparkHiveJdbcStrategy(sparkConf: SparkConf) extends ISessionStrategy {



  /**
    * 会话配置初始化
    *
    * @param sparkSqlCondition 配置项
    **/
  override def sessionInit(sparkSqlCondition: SparkSqlCondition): SparkSession = {
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    sparkSession
  }
}

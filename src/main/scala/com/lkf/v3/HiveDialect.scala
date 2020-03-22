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

import java.sql.{Connection, Statement}

import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types.{DataType, MetadataBuilder}

/**
  * hive jdbc 方言
  **/
private object HiveDialect extends JdbcDialect {
  override def canHandle(url: String): Boolean = url.startsWith("jdbc:hive2")

  override def quoteIdentifier(colName: String): String = {
    s"`$colName`"
  }

  /**
    * 拉取数据之前，覆盖连接信息或配置信息，或其他操作
    *
    * @param connection 连接对象
    * @param properties 连接属性
    */
  override def beforeFetch(connection: Connection, properties: Map[String, String]): Unit = {
    val st: Statement = connection.createStatement()
//    st.execute("set mem_limit=1G")
  }

  override def getCatalystType(sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    None
  }
}

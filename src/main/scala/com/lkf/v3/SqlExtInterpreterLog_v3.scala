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

class SqlExtInterpreterLog_v3 {
  /**
    * 会话ID
    */
  var sessionId: String = ""
  /**
    * 跟踪ID
    */
  var tracId: String = ""
  /**
    * 进入拦截器时间
    */
  var beginTime: String = ""
  /**
    * 结束时间
    */
  var endTime: String = ""
  /**
    * 错误信息
    */
  var msg: String = "ok"

  /**
    * 主体SQL语句
    */
  var mainSql: String = ""
  /**
    * 计算耗时
    */
  var totalElapsedTime: Long = 0

  /**
    * SQL执行耗时
    */
  var sqlExecuteElapsedTime: Long = 0

  /**
    * 结果解析耗时
    */
  var resultParseElapsedTime: Long = 0

  /**
    * 同环比计算耗时
    */
  var qoqElapsedTime: Long = 0

  /**
    * 对比计算耗时
    */
  var comparePivotElapsedTime: Long = 0
}

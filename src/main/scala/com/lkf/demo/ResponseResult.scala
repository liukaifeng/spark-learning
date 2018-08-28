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
package com.lkf.demo

import org.codehaus.jackson.annotate.JsonProperty

class ResponseResult {
  @JsonProperty("msg")
  private[this] var msg: String = ""

  @JsonProperty("xAxis")
  private var xAxis: List[XAxis] = List()

  @JsonProperty("yAxis")
  private var yAxis: List[YAxis] = List()

  @JsonProperty("total")
  private var total: Long = 0

  def setXAxis(xAxis: List[XAxis]): ResponseResult = {
    this.xAxis = xAxis
    this
  }

  def setYAxis(yAxis: List[YAxis]): ResponseResult = {
    this.yAxis = yAxis
    this
  }

  def setTotal(total: Long): ResponseResult = {
    this.total = total
    this
  }

  def getXAxis(): List[XAxis] = {
    this.xAxis
  }

  def getYAxis(): List[YAxis] = {
    this.yAxis
  }

  def getMsg: String = {
    this.msg
  }

  def setMsg(msg: String): Unit = {
    this.msg = msg
  }

  def getTotal(): Long = {
    this.total
  }
}

class XAxis {
  var name: String = ""
  var data: List[String] = _
}

class YAxis {
  var name: String = ""
  var data: List[String] = _
}
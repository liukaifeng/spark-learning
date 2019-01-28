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

package org.apache.livy.client.ext.model;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

/**
 * @package: cn.com.tcsl.loongboss.bigscreen.api.report.model
 * @class-name: IndexConditionBean
 * @description: 指标条件
 * @author: 刘凯峰
 * @date: 2018/3/29 15:17
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class IndexConditionBean extends BaseConditionBean {

    /**
     * 聚合代码，eg：SUM
     */
    private String aggregator;
    /**
     * 聚合名称，eg：求和
     */
    private String aggregatorName;
    /**
     * 别名，eg：金额(求和)
     */
    private String aliasName;
    /**
     * 字段值
     */
    private List<String> fieldValue;

    /**
     * 同环比类型
     * 1-同比；2-环比
     */
    private int qoqType;
    /**
     * 同环比条件
     */
    private QoqConditionBean qoqConditionBean;

}


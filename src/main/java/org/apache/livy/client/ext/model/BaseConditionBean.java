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


/**
 * @package: cn.com.tcsl.loongboss.bigscreen.api.report.model
 * @project-name: tcsl-loongboss-parent
 * @description: 请求条件基类
 * @author: Created by 刘凯峰
 * @create-datetime: 2018-04-04 16-22
 */
@Data
public class BaseConditionBean {
    /**
     * 字段ID
     */
    private String fieldId;
    /**
     * 数据表字段名
     */
    private String fieldName;

    /**
     * 数据表字段别名
     */
    private String fieldAliasName;
    /**
     * 字段名称(中文名称),
     */
    private String fieldDescription;
    /**
     * 别名
     */
    private String aliasName;
    /**
     * 字段的数据类型
     */
    private String dataType;
    /**
     * 字段的数据类型
     */
    private String originDataType;
    /**
     * 使用自定义函数标识
     * 0-默认值（不使用）
     * 1-自定义排序字段转换函数（to_orderby）
     */
    private int udfType;

    /**
     * 表达式类型
     * 0-默认值
     * 1-自定义组合字段
     * 2-自定义计算字段
     */
    private int isBuildAggregated;

    /**
     * 字段计算公式
     */
    private String fieldFormula;

    /**
     * 年季月周标识
     */
    private String granularity;

    /**
     * 同环比类型
     * 1-同比；2-环比
     */
    private int qoqType;


}

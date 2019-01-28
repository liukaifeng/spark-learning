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

/**
 * 同环比条件
 *
 * @author kaifeng
 */
public class QoqConditionBean {
    /**
     * 同环比时间字段名
     */
    private String fieldName;
    /**
     * 同环比时间字段别名
     */
    private String fieldAliasName;
    /**
     * 同环比时间字段名(中文名称),
     */
    private String fieldDescription;

    /**
     * 同环比结果类型
     * 1-增长值；2-增长率
     */
    private int qoqResultType;

    /**
     * 同环比基数时间（逗号隔开）
     * granularity 日：yyyy-MM-dd
     * granularity 周：yyyy-MM-dd
     * granularity 月：yyyy-MM
     * granularity 年：yyyy
     */
    private String qoqRadixTime;

    /**
     * 同环比对比时间（逗号隔开）
     * 时间格式同【qoqRadixTime】
     */
    private String qoqReducedTime;

    /**
     * 年季月周标识
     */
    private String granularity;

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName( String fieldName ) {
        this.fieldName = fieldName;
    }

    public String getFieldDescription() {
        return fieldDescription;
    }

    public void setFieldDescription( String fieldDescription ) {
        this.fieldDescription = fieldDescription;
    }

    public int getQoqResultType() {
        return qoqResultType;
    }

    public void setQoqResultType( int qoqResultType ) {
        this.qoqResultType = qoqResultType;
    }

    public String getQoqRadixTime() {
        return qoqRadixTime;
    }

    public void setQoqRadixTime( String qoqRadixTime ) {
        this.qoqRadixTime = qoqRadixTime;
    }

    public String getQoqReducedTime() {
        return qoqReducedTime;
    }

    public void setQoqReducedTime( String qoqReducedTime ) {
        this.qoqReducedTime = qoqReducedTime;
    }

    public String getGranularity() {
        return granularity;
    }

    public void setGranularity( String granularity ) {
        this.granularity = granularity;
    }

    public String getFieldAliasName() {
        return fieldAliasName;
    }

    public void setFieldAliasName( String fieldAliasName ) {
        this.fieldAliasName = fieldAliasName;
    }
}

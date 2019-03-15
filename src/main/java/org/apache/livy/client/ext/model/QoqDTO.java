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
 * 同环比计算条件
 *
 * @author Created by 刘凯峰
 * @date 2018-08-15 16-03
 */

public class QoqDTO extends QoqConditionBean {
    /**
     * 结果集中是否保留比较的日期字段
     * false-保留；true-删除
     */
    private Boolean delQoqField;

    /**
     * 同环比时间别名
     */
    private String qoqTimeAliasName;


    /**
     * 同环比指标别名
     */
    private String qoqIndexAliasName;


    /**
     * 同环比类型
     * 1-同比；2-环比
     */
    private int qoqType;


    /**
     * 同环比日期格式
     */
    private String qoqJoinOn;


    /**
     * join 表别名
     */
    private String tableAlias;

    /**
     * 同环比计算字段表达式
     */
    private String fieldFormula;


    public QoqDTO() {
        this.delQoqField = false;
    }

    public String getTableAlias() {
        return tableAlias;
    }

    public void setTableAlias( String tableAlias ) {
        this.tableAlias = tableAlias;
    }

    public Boolean getDelQoqField() {
        return delQoqField;
    }

    public void setDelQoqField( Boolean delQoqField ) {
        this.delQoqField = delQoqField;
    }

    public String getQoqTimeAliasName() {
        return qoqTimeAliasName;
    }

    public void setQoqTimeAliasName( String qoqTimeAliasName ) {
        this.qoqTimeAliasName = qoqTimeAliasName;
    }

    public String getQoqIndexAliasName() {
        return qoqIndexAliasName;
    }

    public void setQoqIndexAliasName( String qoqIndexAliasName ) {
        this.qoqIndexAliasName = qoqIndexAliasName;
    }

    public int getQoqType() {
        return qoqType;
    }

    public void setQoqType( int qoqType ) {
        this.qoqType = qoqType;
    }


    public String getQoqJoinOn() {
        return qoqJoinOn;
    }

    public void setQoqJoinOn( String qoqJoinOn ) {
        this.qoqJoinOn = qoqJoinOn;
    }

    public String getFieldFormula() {
        return fieldFormula;
    }

    public void setFieldFormula( String fieldFormula ) {
        this.fieldFormula = fieldFormula;
    }

}

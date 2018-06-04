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

import java.util.List;
import java.util.Map;

/**
 * @package: com.spark.model
 * @project-name: spark-learning
 * @description: spark sql 查询条件实体
 * @author: Created by 刘凯峰
 * @create-datetime: 2018-04-14 13-19
 */
public class SparkSqlCondition {
    /**
     * sql语句
     */
    private String selectSql;
    /**
     * 查询类型
     * 0-默认值（普通查询）
     * 1-筛选项数据查询
     */
    private int queryType;
    /**
     * 查询字段
     */
    private List<String> selectList;
    /**
     * 查询指标项字段
     */
    private List<String> indexList;
    /**
     * 求和字段
     */
    private List<String> sumList;
    /**
     * 分组字段
     */
    private List<String> groupList;
    /**
     * 维度条件字段
     */
    private List<String> dimensionList;

    /**
     * 对比字段
     */
    private List<String> compareList;


    /**
     * 字段名与中文名对应关系
     */
    private Map<String, String> fieldMap;

    /**
     * 排序字段
     */
    private Map<String, List<String>> orderMap;
    /**
     * 聚合字段
     */
    private Map<String, List<String>> aggMap;

    /**
     * spark配置参数
     */
    private Map<String, String> sparkConfig;


    /**
     * 结果数量限制
     */
    private int limit;

    public String getSelectSql() {
        return selectSql;
    }

    public void setSelectSql(String selectSql) {
        this.selectSql = selectSql;
    }

    public List<String> getSelectList() {
        return selectList;
    }

    public void setSelectList(List<String> selectList) {
        this.selectList = selectList;
    }

    public List<String> getIndexList() {
        return indexList;
    }

    public void setIndexList(List<String> indexList) {
        this.indexList = indexList;
    }

    public List<String> getSumList() {
        return sumList;
    }

    public void setSumList(List<String> sumList) {
        this.sumList = sumList;
    }

    public List<String> getGroupList() {
        return groupList;
    }

    public void setGroupList(List<String> groupList) {
        this.groupList = groupList;
    }

    public List<String> getDimensionList() {
        return dimensionList;
    }

    public void setDimensionList(List<String> dimensionList) {
        this.dimensionList = dimensionList;
    }

    public List<String> getCompareList() {
        return compareList;
    }

    public void setCompareList(List<String> compareList) {
        this.compareList = compareList;
    }

    public Map<String, List<String>> getOrderMap() {
        return orderMap;
    }

    public void setOrderMap(Map<String, List<String>> orderMap) {
        this.orderMap = orderMap;
    }

    public Map<String, List<String>> getAggMap() {
        return aggMap;
    }

    public void setAggMap(Map<String, List<String>> aggMap) {
        this.aggMap = aggMap;
    }

    public Map<String, String> getSparkConfig() {
        return sparkConfig;
    }

    public void setSparkConfig(Map<String, String> sparkConfig) {
        this.sparkConfig = sparkConfig;
    }

    public Map<String, String> getFieldMap() {
        return fieldMap;
    }

    public void setFieldMap(Map<String, String> fieldMap) {
        this.fieldMap = fieldMap;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }


    public int getQueryType() {
        return queryType;
    }

    public void setQueryType(int queryType) {
        this.queryType = queryType;
    }
}

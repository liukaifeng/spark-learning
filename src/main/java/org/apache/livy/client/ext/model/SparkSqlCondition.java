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

import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * @package: com.spark.model
 * @project-name: spark-learning
 * @description: spark sql 查询条件实体
 * @author: Created by 刘凯峰
 * @create-datetime: 2018-04-14 13-19
 */
public class SparkSqlCondition extends BaseBuilder {
    /**
     * sql语句
     */
    private String selectSql;


    /**
     * 结果集中是否清除自定义字段列
     */
    private Boolean delFilterField;

    /**
     * 查询字段
     */
    private List<String> selectList;

    /**
     * 查询指标项字段
     */
    private List<String> indexList;

    /**
     * 分组字段
     */
    private List<String> groupList;

    /**
     * 对比字段
     */
    private List<String> compareList;

    /**
     * 自定义组合字段作为筛选项
     */
    private List<String> filterCustomFieldList;

    /**
     * 自定义字段筛选表达式
     */
    private String filterFormula;

    /**
     * 排序字段
     */
    private Map<String, String> orderByMap;
    /**
     * spark配置参数
     */
    private Map<String, String> sparkConfig;

    /**
     * 聚合信息
     */
    private Map<String, List<String>> sparkAggMap;

    /**
     * 字段名与中文名对应关系
     */
    private Map<String, String> fieldAndDescMap = Maps.newLinkedHashMap();

    /**
     * 字段与别名对应关系
     */
    private Map<String, String> fieldAndAliasMap = Maps.newLinkedHashMap();

    /**
     * 交叉表排序
     */
    private Map<String, String> crosstabByMap = Maps.newLinkedHashMap();

    public List<String> getFilterCustomFieldList() {
        return filterCustomFieldList;
    }

    public void setFilterCustomFieldList( List<String> filterCustomFieldList ) {
        this.filterCustomFieldList = filterCustomFieldList;
    }

    public String getSelectSql() {
        return selectSql;
    }

    public void setSelectSql( String selectSql ) {
        this.selectSql = selectSql;
    }

    public List<String> getSelectList() {
        return selectList;
    }

    public void setSelectList( List<String> selectList ) {
        this.selectList = selectList;
    }

    public List<String> getIndexList() {
        return indexList;
    }

    public void setIndexList( List<String> indexList ) {
        this.indexList = indexList;
    }

    public List<String> getGroupList() {
        return groupList;
    }

    public void setGroupList( List<String> groupList ) {
        this.groupList = groupList;
    }

    public List<String> getCompareList() {
        return compareList;
    }

    public void setCompareList( List<String> compareList ) {
        this.compareList = compareList;
    }

    public Map<String, String> getOrderByMap() {
        return orderByMap;
    }

    public void setOrderByMap( Map<String, String> orderByMap ) {
        this.orderByMap = orderByMap;
    }

    public Map<String, String> getSparkConfig() {
        return sparkConfig;
    }

    public void setSparkConfig( Map<String, String> sparkConfig ) {
        this.sparkConfig = sparkConfig;
    }

    public Map<String, List<String>> getSparkAggMap() {
        return sparkAggMap;
    }

    public void setSparkAggMap( Map<String, List<String>> sparkAggMap ) {
        this.sparkAggMap = sparkAggMap;
    }

    public Map<String, String> getFieldAndDescMap() {
        return fieldAndDescMap;
    }

    public void setFieldAndDescMap( Map<String, String> fieldAndDescMap ) {
        this.fieldAndDescMap = fieldAndDescMap;
    }

    public Map<String, String> getFieldAndAliasMap() {
        return fieldAndAliasMap;
    }

    public void setFieldAndAliasMap( Map<String, String> fieldAndAliasMap ) {
        this.fieldAndAliasMap = fieldAndAliasMap;
    }

    public Map<String, String> getCrosstabByMap() {
        return crosstabByMap;
    }

    public void setCrosstabByMap( Map<String, String> crosstabByMap ) {
        this.crosstabByMap = crosstabByMap;
    }

    public Boolean getDelFilterField() {
        return delFilterField;
    }

    public void setDelFilterField( Boolean delFilterField ) {
        this.delFilterField = delFilterField;
    }


    public String getFilterFormula() {
        return filterFormula;
    }

    public void setFilterFormula( String filterFormula ) {
        this.filterFormula = filterFormula;
    }
}

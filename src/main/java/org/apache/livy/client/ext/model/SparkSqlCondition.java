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
     * 对比字段
     */
    private List<String> compareList;

    /**
     * 排序字段
     */
    private List<Map<String, String>> orderList;

    /**
     * spark配置参数
     */
    private Map<String, String> sparkConfig;


    public String getSelectSql() {
        return selectSql;
    }

    public void setSelectSql(String selectSql) {
        this.selectSql = selectSql;
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

    public List<String> getCompareList() {
        return compareList;
    }

    public void setCompareList(List<String> compareList) {
        this.compareList = compareList;
    }

    public List<Map<String, String>> getOrderList() {
        return orderList;
    }

    public void setOrderList(List<Map<String, String>> orderList) {
        this.orderList = orderList;
    }

    private SparkSqlCondition() {
    }

    public static SparkSqlCondition build() {
        return new SparkSqlCondition();
    }

    public Map<String, String> getSparkConfig() {
        return sparkConfig;
    }

    public void setSparkConfig(Map<String, String> sparkConfig) {
        this.sparkConfig = sparkConfig;
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
}

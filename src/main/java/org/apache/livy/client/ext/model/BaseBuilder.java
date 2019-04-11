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

import java.util.Map;

/**
 * sql拼接基类
 *
 * @author Created by 刘凯峰
 * @date 2018-06-22 13-49
 */
public class BaseBuilder {
    /**
     * 查询类型
     * 0-默认值（普通查询）
     * 1-筛选项数据查询
     * 2-导出数据查询
     */
    private int queryType;
    /**
     * 返回数据条件
     * 0-全部
     * 1-前几条
     * 2-后几条
     */
    private int queryPoint;

    /**
     * 当前页
     */
    private Integer page;

    /**
     * 返回条数
     */
    private Integer limit;

    /**
     * 数据源类型(0-默认；1-cassandra)
     */
    private int dataSourceType;

    /**
     * 数据表
     */
    private String table;
    /**
     * 建空间
     */
    private String keyspace;
    /**
     * kudu 连接地址
     */
    private String kuduMaster;

    /**
     * 会话ID
     */
    private String sessionId;
    /**
     * 命令ID
     */
    private String tracId;

    /**
     * 对比项是否需要排序标识
     */
    private Boolean compareSortFlag;

    /**
     * spark 配置信息
     */
    private Map<String, String> sparkConfigMap;

    /**
     * mongodb 配置
     */
    private Map<String, String> mongoConfigMap;

    /***
     * hive jdbc 配置
     * */
    private Map<String, String> hiveJdbcConfig;


    /**
     * 是否需要二次计算标识
     */
    private Boolean secondaryFlag = false;

    /**
     * 是否有维度条件
     */
    private Boolean dimensionIsExists = true;

    public void setLimit( Integer limit ) {
        this.limit = limit;
    }

    public Boolean getSecondaryFlag() {
        return secondaryFlag;
    }

    public void setSecondaryFlag( Boolean secondaryFlag ) {
        this.secondaryFlag = secondaryFlag;
    }


    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public int getQueryType() {
        return queryType;
    }

    public void setQueryType(int queryType) {
        this.queryType = queryType;
    }

    public int getQueryPoint() {
        return queryPoint;
    }

    public void setQueryPoint(int queryPoint) {
        this.queryPoint = queryPoint;
    }


    public int getDataSourceType() {
        return dataSourceType;
    }

    public void setDataSourceType(int dataSourceType) {
        this.dataSourceType = dataSourceType;
    }

    public Integer getPage() {
        return page;
    }

    public void setPage(Integer page) {
        this.page = page;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }


    public String getTracId() {
        return tracId;
    }

    public void setTracId(String tracId) {
        this.tracId = tracId;
    }

    public Boolean getCompareSortFlag() {
        return compareSortFlag;
    }

    public void setCompareSortFlag(Boolean compareSortFlag) {
        this.compareSortFlag = compareSortFlag;
    }

    public BaseBuilder() {
        this.compareSortFlag = false;
    }

    public Map<String, String> getSparkConfigMap() {
        return sparkConfigMap;
    }

    public void setSparkConfigMap(Map<String, String> sparkConfigMap) {
        this.sparkConfigMap = sparkConfigMap;
    }

    public Map<String, String> getMongoConfigMap() {
        return mongoConfigMap;
    }

    public void setMongoConfigMap(Map<String, String> mongoConfigMap) {
        this.mongoConfigMap = mongoConfigMap;
    }

    public String getKuduMaster() {
        return kuduMaster;
    }

    public void setKuduMaster(String kuduMaster) {
        this.kuduMaster = kuduMaster;
    }

    public Boolean getDimensionIsExists() {
        return dimensionIsExists;
    }

    public void setDimensionIsExists( Boolean dimensionIsEmpty ) {
        this.dimensionIsExists = dimensionIsEmpty;
    }

    public Map<String, String> getHiveJdbcConfig() {
        return hiveJdbcConfig;
    }

    public void setHiveJdbcConfig( Map<String, String> hiveJdbcConfig ) {
        this.hiveJdbcConfig = hiveJdbcConfig;
    }

    public Integer getLimit() {
        return limit;
    }
}

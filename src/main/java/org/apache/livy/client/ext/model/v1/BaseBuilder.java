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
package org.apache.livy.client.ext.model.v1;

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

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
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
}

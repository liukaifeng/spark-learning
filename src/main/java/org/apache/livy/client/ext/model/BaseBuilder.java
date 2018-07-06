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
 * @package: org.apache.livy.client.ext.model
 * @project-name: spark-learning
 * @description: sql拼接基类
 * @author: Created by 刘凯峰
 * @create-datetime: 2018-06-22 13-49
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
     * 结果数量限制
     */
    private int limit;

    public int getQueryType() {
        return queryType;
    }

    public void setQueryType( int queryType ) {
        this.queryType = queryType;
    }

    public int getQueryPoint() {
        return queryPoint;
    }

    public void setQueryPoint( int queryPoint ) {
        this.queryPoint = queryPoint;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit( int limit ) {
        this.limit = limit;
    }
}

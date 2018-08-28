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

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;
import java.util.Map;

/**
 * 报表统计参数入参
 *
 * @author Created by 刘凯峰
 * @date 2018-03-29 10-06
 */
@EqualsAndHashCode()
@Data
public class BiReportBuildInDTO {
    /**
     * 默认当前页
     */
    private Integer page;
    /**
     * 默认返回条数
     */
    private Integer limit;
    /**
     * 默认返回条数
     */
    private String dbName;
    /**
     * 默认返回条数
     */
    private String tbName;

    /**
     * 返回数据条件
     * 0-全部
     * 1-前几条
     * 2-后几条
     */
    private Integer queryPoint;
    /**
     * 查询类型
     * 0-默认值（普通查询）
     * 1-筛选项数据查询
     */
    private Integer queryType;

    /**
     * 数据源类型(0-默认；1-cassandra)
     */
    private Integer dataSourceType;

    /**
     * 过滤条件
     */
    private List<FilterConditionBean> filterCondition;
    /**
     * 维度条件
     */
    private List<DimensionConditionBean> dimensionCondition;
    /**
     * 对比条件
     */
    private List<CompareConditionBean> compareCondition;
    /**
     * 指标条件
     */
    private List<IndexConditionBean> indexCondition;
    /**
     * 排序条件
     */
    private List<SortConditionBean> sortCondition;

    /**
     * spark配置参数
     */
    private Map<String, String> sparkConfig;

    public BiReportBuildInDTO() {
        this.page = 1;
        this.limit = 0;
        this.queryPoint = 0;
        this.queryType = 0;
        this.dataSourceType = 0;
    }

}

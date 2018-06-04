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

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

/**
 * @package: org.apache.livy.repl.ext.model;
 * @project-name: spark-learning
 * @description: 根据条件组装sparksql
 * @author: Created by 刘凯峰
 * @create-datetime: 2018-04-14 11-11
 */
public class SparkSqlBuild {
    private static Logger logger = LoggerFactory.getLogger(SparkSqlBuild.class);

    public  SparkSqlCondition buildSqlStatement( String param ) {
        BiReportBuildInDTO biReportBuildInDTO = JSONObject.parseObject(param, BiReportBuildInDTO.class);
        SqlConditionBuilder sqlConditionBuilder = new SqlConditionBuilder();
        sqlConditionBuilder.sparkConfigBuilder(biReportBuildInDTO.getSparkConfig());

        //指标条件-拼成select条件
        List<IndexConditionBean> indexConditionBeanList = biReportBuildInDTO.getIndexCondition();
        //构造select条件
        indexConditionBeanList.forEach(index -> {
            sqlConditionBuilder.indexBuilder(index.getFieldName());
            sqlConditionBuilder.selectBuilder(index.getFieldName());
            sqlConditionBuilder.selectSqlBuilder(index.getFieldName(),
                    index.getDataType(),
                    index.getAggregator().toLowerCase()
                    , index.getAliasName());
        });
        //筛选条件-拼成where条件
        List<FilterConditionBean> filterConditionBeanList = biReportBuildInDTO.getFilterCondition();
        //构造where条件
        if (filterConditionBeanList != null && !filterConditionBeanList.isEmpty()) {
            filterConditionBeanList.forEach(filterConditionBean -> {
                logger.info("【SparkSqlBuild::SparkSqlCondition】-where条件：" + JSONObject.toJSONString(filterConditionBean));
                sqlConditionBuilder.whereBuilder(filterConditionBean.getFieldName(), filterConditionBean.getFieldValue(), filterConditionBean.getDataType());
            });
        }
        //维度条件-拼成group条件
        List<DimensionConditionBean> dimensionConditionBeanList = biReportBuildInDTO.getDimensionCondition();
        List<String> groupSqlList = Lists.newArrayList();
        if (Objects.nonNull(dimensionConditionBeanList) && !dimensionConditionBeanList.isEmpty()) {
            dimensionConditionBeanList.forEach(dimensionConditionBean -> {
                groupSqlList.add(dimensionConditionBean.getFieldName());
                sqlConditionBuilder.selectSqlBuilder(dimensionConditionBean.getFieldName(), dimensionConditionBean.getDataType(), "", "");
                sqlConditionBuilder.selectBuilder(dimensionConditionBean.getFieldName());
                sqlConditionBuilder.groupSparkBuilder(dimensionConditionBean.getFieldName());
            });
        }
        //对比条件
        List<CompareConditionBean> compareConditionList = biReportBuildInDTO.getCompareCondition();
        if (Objects.nonNull(compareConditionList) && !compareConditionList.isEmpty()) {
            logger.info("【SparkSqlBuild::SparkSqlCondition】-进入对比条件");
            String split = " || ':%' || ";
            StringBuilder compareFieldNameBuilder = new StringBuilder();
            compareConditionList.forEach(compareConditionBean -> {
                sqlConditionBuilder.selectBuilder(compareConditionBean.getFieldName());
                compareFieldNameBuilder.append(compareConditionBean.getFieldName()).append(split);
                sqlConditionBuilder.compareBuilder(compareConditionBean.getFieldName());
            });
            if (!Strings.isNullOrEmpty(compareFieldNameBuilder.toString())) {
                String compareFieldName = compareFieldNameBuilder.substring(0, compareFieldNameBuilder.length() - split.length());
                logger.info("【SparkSqlBuild::SparkSqlCondition】-对比条件：" + compareFieldNameBuilder.toString());
                groupSqlList.add("y");
                sqlConditionBuilder.selectSqlBuilder(compareFieldName + " as y ", "str", "", "");
            }
        }
        logger.info("【SparkSqlBuild::SparkSqlCondition】-分组条件：" + JSONObject.toJSONString(groupSqlList));

        if (!groupSqlList.isEmpty()) {
            sqlConditionBuilder.groupSqlBuilder(groupSqlList);
        }
        //排序条件-拼成orderBy条件
        List<SortConditionBean> sortConditionBeanList = biReportBuildInDTO.getSortCondition();
        if (Objects.nonNull(sortConditionBeanList) && !sortConditionBeanList.isEmpty()) {
            sortConditionBeanList.forEach(sortConditionBean -> {
                sqlConditionBuilder.orderByBuilder(sortConditionBean.getFieldName(), sortConditionBean.getSortFlag());
            });
        }
        sqlConditionBuilder.limitBuilder(biReportBuildInDTO.getLimit());
        sqlConditionBuilder.tableBuilder(biReportBuildInDTO.getDbName(), biReportBuildInDTO.getTbName());
        return SearchSqlBuilder.toSparkSql(sqlConditionBuilder);
    }


}

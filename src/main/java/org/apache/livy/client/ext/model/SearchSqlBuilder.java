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


import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.livy.client.ext.model.Constant.*;


/**
 * @package: cn.com.tcsl.loongboss.bigscreen.biz.corp.service.impl
 * @project-name: tcsl-loongboss-parent
 * @description: 查询sql构造
 * @author: Created by 刘凯峰
 * @create-datetime: 2018-03-30 09-30
 */
public class SearchSqlBuilder {

    private static final String SELECT = "select ";
    private static final String FROM = " from ";
    private static final String WHERE = " where ";
    private static final String LIMIT_ITEM = " limit ";
    //聚合条件(sum,count,avg)
    private final Map<String, List<String>> aggMap = Maps.newConcurrentMap();
    //排序条件
    private final Map<String, List<String>> orderMap = Maps.newConcurrentMap();

    /**
     * @method-name: selectBuilder
     * @description: 构造sql的select
     * @author: 刘凯峰
     * @date: 2018/3/30 14:59
     * @param: [sqlConditionBuilder]
     * @return: java.lang.String
     * @version V1.0
     * update-logs:方法变更说明
     * ****************************************************
     * name:
     * date:
     * description:
     * *****************************************************
     */
    private String selectBuilder( SqlConditionBuilder sqlConditionBuilder ) {
        StringBuilder stringBuilder = new StringBuilder(SELECT);
        List<String> selectSqlList = sqlConditionBuilder.getSelectSqlList();
        int size = selectSqlList.size();

        if (size > 0) {
            selectSqlList.forEach(select -> {
                stringBuilder.append(select).append(",");
            });
        }
        return stringBuilder.substring(0, stringBuilder.length() - 1);
    }

    private String tableBuilder( SqlConditionBuilder sqlConditionBuilder ) {
        return FROM + sqlConditionBuilder.getTableName();
    }

    /**
     * @method-name: whereBuilder
     * @description: 构造sql的where
     * @author: 刘凯峰
     * @date: 2018/3/30 15:00
     * @param: [sqlConditionBuilder]
     * @return: java.lang.String
     * @version V1.0
     * update-logs:方法变更说明
     * ****************************************************
     * name:
     * date:
     * description:
     * *****************************************************
     */
    private String whereBuilder( SqlConditionBuilder sqlConditionBuilder ) {
        StringBuilder stringBuilder = new StringBuilder(WHERE);
        List<String> whereList = sqlConditionBuilder.getWhereList();
        List<String> selectList = sqlConditionBuilder.getSelectList();
        int size = whereList.size();
        String and = " and ";
        String notNull = " IS NOT NULL ";
        if (size > 0) {
            stringBuilder.append(whereList.parallelStream().collect(Collectors.joining(and)));
        }
        if (!selectList.isEmpty() && selectList.size() > 0) {
            selectList.forEach(s -> {
                stringBuilder.append(and).append(s).append(notNull);
            });
        }
        return stringBuilder.toString();
    }

    /**
     * @method-name: groupBuilder
     * @description: 构造sql的分组条件
     * @author: 刘凯峰
     * @date: 2018/3/30 15:01
     * @param: [sqlConditionBuilder]
     * @return: java.lang.String
     * @version V1.0
     * update-logs:方法变更说明
     * ****************************************************
     * name:
     * date:
     * description:
     * *****************************************************
     */
    private List<String> groupBuilder( SqlConditionBuilder sqlConditionBuilder ) {
        if (Objects.nonNull(sqlConditionBuilder.getGroupByList()) && !sqlConditionBuilder.getGroupByList().isEmpty()) {
            return sqlConditionBuilder.getGroupByList();
        }
        return Lists.newArrayList();
    }

    private List<String> compareBuilder( SqlConditionBuilder sqlConditionBuilder ) {
        return sqlConditionBuilder.getCompareList();
    }

    /**
     * @method-name: orderBuilder
     * @description: 构造sql的排序条件
     * @author: 刘凯峰
     * @date: 2018/3/30 15:01
     * @param: [sqlConditionBuilder]
     * @return: java.lang.String
     * @version V1.0
     * update-logs:方法变更说明
     * ****************************************************
     * name:
     * date:
     * description:
     * *****************************************************
     */
    private Map<String, List<String>> orderBuilder( SqlConditionBuilder sqlConditionBuilder ) {
        //升序排序项
        if (Objects.nonNull(sqlConditionBuilder.getAscList()) && !sqlConditionBuilder.getAscList().isEmpty()) {
            orderMap.put(Constant.SORT_ASC, sqlConditionBuilder.getAscList());
        }
        //降序排序项
        if (Objects.nonNull(sqlConditionBuilder.getDescList()) && !sqlConditionBuilder.getDescList().isEmpty()) {
            orderMap.put(Constant.SORT_DESC, sqlConditionBuilder.getDescList());
        }
        //未指定排序项
        if (orderMap.isEmpty()) {
            int queryPoint = sqlConditionBuilder.getQueryPoint();
            String sort = Constant.SORT_ASC;
            if (queryPoint == 2) {
                sort = Constant.SORT_DESC;
            }
            //维度条件
            List<String> groupByList = sqlConditionBuilder.getGroupByList();
            //计算字段
            List<String> sumList = sqlConditionBuilder.getSumList();
            //优先使用计算字段排序
            if (Objects.nonNull(sumList) && !sumList.isEmpty()) {
                orderMap.put(sort, Lists.newArrayList(sumList.get(0)));
            } else if (Objects.nonNull(groupByList) && !groupByList.isEmpty()) {
                orderMap.put(sort, Lists.newArrayList(groupByList.get(0)));
            }
        }
        return orderMap;
    }

    private Map<String, List<String>> aggBuilder( SqlConditionBuilder sqlConditionBuilder ) {
        if (Objects.nonNull(sqlConditionBuilder.getSumList()) && !sqlConditionBuilder.getSumList().isEmpty()) {
            aggMap.put(AGG_SUM, sqlConditionBuilder.getSumList());
        }
        if (Objects.nonNull(sqlConditionBuilder.getCountList()) && !sqlConditionBuilder.getCountList().isEmpty()) {
            aggMap.put(AGG_COUNT, sqlConditionBuilder.getCountList());
        }
        if (Objects.nonNull(sqlConditionBuilder.getAvgList()) && !sqlConditionBuilder.getAvgList().isEmpty()) {
            aggMap.put(AGG_AVG, sqlConditionBuilder.getAvgList());
        }
        return aggMap;
    }

    private String limitBuilder( SqlConditionBuilder sqlConditionBuilder ) {
        String limitResult = "";
        if (!Strings.isNullOrEmpty(sqlConditionBuilder.getLimit())) {
            limitResult = sqlConditionBuilder.getLimit();
        }
        return limitResult;
    }

    /**
     * @method-name: toSql
     * @description: 构造完整SQL
     * @author: 刘凯峰
     * @date: 2018/3/30 15:02
     * @param: [sqlConditionBuilder]
     * @return: java.lang.String
     * @version V1.0
     * update-logs:方法变更说明
     * ****************************************************
     * name:
     * date:
     * description:
     * *****************************************************
     */
    public SparkSqlCondition toSparkSql( SqlConditionBuilder sqlConditionBuilder ) {
        SparkSqlCondition sparkSqlCondition = new SparkSqlCondition();
        StringBuilder sqlBuilder = new StringBuilder();
        //select
        sqlBuilder.append(selectBuilder(sqlConditionBuilder));
        sqlBuilder.append(tableBuilder(sqlConditionBuilder));
        //where
        String where = whereBuilder(sqlConditionBuilder);
        //group
        List<String> groupList = groupBuilder(sqlConditionBuilder);
        //compare
        List<String> compare = compareBuilder(sqlConditionBuilder);
        //order by
        Map<String, List<String>> orderByMap = orderBuilder(sqlConditionBuilder);
        //limit
        String limit = limitBuilder(sqlConditionBuilder);
        //聚合条件
        Map<String, List<String>> aggMap = aggBuilder(sqlConditionBuilder);
        if (!Strings.isNullOrEmpty(where)) {
            sqlBuilder.append(where);
        }
        if (Objects.nonNull(groupList) && !groupList.isEmpty()) {
            sparkSqlCondition.setGroupList(sqlConditionBuilder.getGroupByList());
        }
        if (Objects.nonNull(orderByMap) && !orderByMap.isEmpty()) {
            sparkSqlCondition.setOrderMap(orderByMap);
        }
        if (compare != null && !compare.isEmpty()) {
            sparkSqlCondition.setCompareList(compare);
        }
        if (Objects.nonNull(aggMap) && !aggMap.isEmpty()) {
            sparkSqlCondition.setAggMap(aggMap);
        }
        if (!Strings.isNullOrEmpty(limit)) {
            sparkSqlCondition.setLimit(Integer.valueOf(limit));
        }
        //完整SQL语句
        sparkSqlCondition.setSelectSql(sqlBuilder.toString());
        sparkSqlCondition.setSelectList(sqlConditionBuilder.getSelectList());
        sparkSqlCondition.setIndexList(sqlConditionBuilder.getIndexList());
        sparkSqlCondition.setDimensionList(sqlConditionBuilder.getDimensionList());
        sparkSqlCondition.setFieldMap(sqlConditionBuilder.getFieldMap());
        //spark配置信息
        sparkSqlCondition.setSparkConfig(sqlConditionBuilder.getSparkConfig());
        sparkSqlCondition.setQueryType(sqlConditionBuilder.getQueryType());
        return sparkSqlCondition;
    }
}

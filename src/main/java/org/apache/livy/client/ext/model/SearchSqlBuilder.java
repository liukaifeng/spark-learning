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

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;


/**
 * @package: cn.com.tcsl.loongboss.bigscreen.biz.corp.service.impl
 * @project-name: tcsl-loongboss-parent
 * @description: 查询sql构造
 * @author: Created by 刘凯峰
 * @create-datetime: 2018-03-30 09-30
 */
public class SearchSqlBuilder {

    private  final String SELECT = "select ";
    private  final String FROM = " from ";
    private  final String WHERE = " where ";
    private  final String GROUP_BY = " group by ";
    private  final String ORDER_BY = " order by ";
    private  final String LIMIT_ITEM = " limit ";

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
    private  String selectBuilder( SqlConditionBuilder sqlConditionBuilder ) {
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

    private  String tableBuilder( SqlConditionBuilder sqlConditionBuilder ) {
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
    private  String whereBuilder( SqlConditionBuilder sqlConditionBuilder ) {
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
            selectList.parallelStream().forEach(s -> {
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
    private  String groupBuilder( SqlConditionBuilder sqlConditionBuilder ) {
        if (!Strings.isNullOrEmpty(sqlConditionBuilder.getGroupBy())) {
            return GROUP_BY + sqlConditionBuilder.getGroupBy();
        }
        return "";
    }

    private  List<String> compareBuilder( SqlConditionBuilder sqlConditionBuilder ) {
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
    private  String orderBuilder( SqlConditionBuilder sqlConditionBuilder ) {
        String dotStr = ",";
        StringBuilder stringBuilder = new StringBuilder(ORDER_BY);
        if (!sqlConditionBuilder.getCompareList().isEmpty()) {
            stringBuilder.append(" y").append(dotStr);
        }
        List<String> orderByList = sqlConditionBuilder.getOrderByList();
        if (orderByList != null && !orderByList.isEmpty()) {
            orderByList.forEach(order -> {
                stringBuilder.append(order).append(dotStr);
            });
        } else {
            //分组条件
            List<String> groupByList = sqlConditionBuilder.getGroupByList();
            //查询字段
            List<String> selectList = sqlConditionBuilder.getSelectList();
            if (groupByList != null && !groupByList.isEmpty()) {
                stringBuilder.append(groupByList.get(0)).append(dotStr);
            } else if (selectList != null && !selectList.isEmpty()) {
                stringBuilder.append(selectList.get(0)).append(dotStr);
            }
        }

        if (Objects.equals(stringBuilder.toString(), ORDER_BY)) {
            return "";
        }
        return stringBuilder.substring(0, stringBuilder.length() - dotStr.length());
    }

    private  List<String> sumBuilder( SqlConditionBuilder sqlConditionBuilder ) {
        return sqlConditionBuilder.getSumList();
    }

    private  String limitBuilder( SqlConditionBuilder sqlConditionBuilder ) {
        String limitResult = "";
        if (!Strings.isNullOrEmpty(sqlConditionBuilder.getLimit())) {
            limitResult = LIMIT_ITEM.concat(sqlConditionBuilder.getLimit());
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
    public  SparkSqlCondition toSparkSql( SqlConditionBuilder sqlConditionBuilder ) {
        SparkSqlCondition sparkSqlCondition = SparkSqlCondition.build();
        StringBuilder sqlBuilder = new StringBuilder();
        //select
        sqlBuilder.append(selectBuilder(sqlConditionBuilder));
        sqlBuilder.append(tableBuilder(sqlConditionBuilder));
        //where
        String where = whereBuilder(sqlConditionBuilder);
        //group
        String group = groupBuilder(sqlConditionBuilder);
        //compare
        List<String> compare = compareBuilder(sqlConditionBuilder);
        //sum
        List<String> sum = sumBuilder(sqlConditionBuilder);
        //order by
        String orderBy = orderBuilder(sqlConditionBuilder);
        //limit
        String limit = limitBuilder(sqlConditionBuilder);
        if (!Strings.isNullOrEmpty(where)) {
            sqlBuilder.append(where);
        }
        if (!Strings.isNullOrEmpty(group)) {
            sqlBuilder.append(group);
            sparkSqlCondition.setGroupList(sqlConditionBuilder.getGroupByList());
        }
        if (!Strings.isNullOrEmpty(orderBy)) {
            sqlBuilder.append(orderBy);
        }
        if (!sum.isEmpty()) {
            sparkSqlCondition.setSumList(sum);
        }
        if (compare != null && !compare.isEmpty()) {
            sparkSqlCondition.setCompareList(compare);
        }
        if (!Strings.isNullOrEmpty(limit)) {
            sqlBuilder.append(limit);
        }
        sparkSqlCondition.setIndexList(sqlConditionBuilder.getIndexList());
        sparkSqlCondition.setSelectList(sqlConditionBuilder.getSelectList());
        sparkSqlCondition.setSelectSql(sqlBuilder.toString());
        //spark配置信息
        sparkSqlCondition.setSparkConfig(sqlConditionBuilder.getSparkConfig());
        return sparkSqlCondition;
    }
}

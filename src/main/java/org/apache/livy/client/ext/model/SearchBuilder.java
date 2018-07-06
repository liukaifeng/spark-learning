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

import static org.apache.livy.client.ext.model.Constant.COMPARE_SPLIT_CHAR;
import static org.apache.livy.client.ext.model.Constant.DataFieldType.STRING_TYPE;


/**
 * @package: cn.com.tcsl.loongboss.bigscreen.biz.corp.service.impl
 * @project-name: tcsl-loongboss-parent
 * @description: 查询sql构造
 * @author: Created by 刘凯峰
 * @create-datetime: 2018-03-30 09-30
 */
public class SearchBuilder {

    private final String SELECT = "select ";
    private final String FROM = " from ";
    private final String WHERE = " where ";
    private final String GROUP_BY = " group by ";
    private final String ORDER_BY = " order by ";
    private final Map<String, String> orderByAliasMap = Maps.newLinkedHashMap();

    /**
     * select 语句拼接
     */
    private String selectBuilder( SqlBuilder sqlbuilder ) {
        StringBuilder stringBuilder = new StringBuilder(SELECT);
        List<String> selectSqlList = sqlbuilder.getSelectSqlList();
        int size = selectSqlList.size();

        if (size > 0) {
            selectSqlList.stream().distinct().forEach(select -> {
                stringBuilder.append(select).append(",");
            });
        }
        return stringBuilder.substring(0, stringBuilder.length() - 1);
    }

    /**
     * 数据表拼接
     */
    private String tableBuilder( SqlBuilder sqlbuilder ) {
        return FROM + sqlbuilder.getTableName();
    }

    /**
     * where语句拼接
     */
    private String whereBuilder( SqlBuilder sqlbuilder ) {
        StringBuilder stringBuilder = new StringBuilder(WHERE);
        List<String> whereList = sqlbuilder.getWhereList();
        List<String> selectList = sqlbuilder.getSelectFieldList();
        Map<String, Integer> fieldAndFormulaTypeMap = sqlbuilder.getFieldAndFormulaTypeMap();
        Map<String, String> fieldAndTypeMap = sqlbuilder.getFieldAndTypeMap();
        int size = whereList.size();
        String and = " and ";
        String notNull = " IS NOT NULL ";
        String notEmpty = "!=''";

        if (size > 0) {
            stringBuilder.append(whereList.stream().distinct().collect(Collectors.joining(and)));
        }
        if (!selectList.isEmpty()) {
            List<String> list = Lists.newArrayList();
            selectList.stream().distinct().forEach(s -> {
                if (fieldAndFormulaTypeMap.get(s) == 0) {
                    list.add(s.concat(notNull));
                    if (STRING_TYPE.getType().equals(fieldAndTypeMap.get(s))) {
                        list.add(s.concat(notEmpty));
                    }
                }
            });
            if (!list.isEmpty()) {
                String selectItem = list.stream().collect(Collectors.joining(and));
                if (stringBuilder.toString().equals(WHERE)) {
                    stringBuilder.append(selectItem);
                } else {
                    stringBuilder.append(and).append(selectItem);
                }
            }
        }
        if (!stringBuilder.toString().equals(WHERE)) {
            return stringBuilder.toString();
        }
        return "";
    }

    /**
     * 构造sql的分组条件
     */
    private String groupBuilder( SqlBuilder sqlBuilder ) {
        List<String> groupSqlList = sqlBuilder.getGroupSqlList();
        if (Objects.nonNull(groupSqlList) && !groupSqlList.isEmpty()) {
            return GROUP_BY + groupSqlList.stream().collect(Collectors.joining(","));
        }
        return "";
    }

    /**
     * 对比条件别名
     */
    private List<String> compareBuilder( SqlBuilder sqlbuilder ) {
        return sqlbuilder.getCompareFieldAliasList();
    }

    /**
     * 排序sql拼接
     * <p>
     * 升序排序取前10条：sql升序，结果升序
     * 升序排序取后10条：sql降序，结果升序
     * 降序排序取后10条：sql升序，结果降序
     * 降序排序取前10条：sql降序，结果降序
     * </p>
     */
    private String orderBuilder( SqlBuilder sqlbuilder ) {
        String dotStr = ",";
        String sort = Constant.SORT_ASC;
        StringBuilder stringBuilder = new StringBuilder(ORDER_BY);
        //排序字段与升降序对应关系
        Map<String, String> orderByMap = sqlbuilder.getOrderByMap();
        //字段与别名对应关系
        Map<String, String> fieldAndAliasMap = sqlbuilder.getFieldAndAliasMap();

        if (orderByMap != null && !orderByMap.isEmpty()) {
            for (Map.Entry<String, String> entry : orderByMap.entrySet()) {
                orderByAliasMap.put(entry.getKey(), entry.getValue());
                if (sqlbuilder.getQueryPoint() == 2) {
                    sort = Constant.SORT_DESC.equals(entry.getValue().toUpperCase()) ? Constant.SORT_ASC : Constant.SORT_DESC;
                } else {
                    sort = entry.getValue();
                }
                stringBuilder.append(entry.getKey()).append(" ").append(sort).append(dotStr);
            }
        } else {
            //指标字段
            List<String> indexList = sqlbuilder.getIndexList();
            String fieldName = "";
            String fieldAliasName = "";

            if (indexList != null && !indexList.isEmpty()) {
                fieldName = indexList.get(0);
                //字段别名
                fieldAliasName = fieldAndAliasMap.get(fieldName);
            }
            if (!Strings.isNullOrEmpty(fieldAliasName)) {
                fieldName = fieldAliasName;
                stringBuilder.append(fieldName).append(" ").append(sort).append(dotStr);
                orderByAliasMap.put(fieldName, sort);
            }
        }
        if (Objects.equals(stringBuilder.toString(), ORDER_BY)) {
            return "";
        }
        return stringBuilder.substring(0, stringBuilder.length() - dotStr.length());
    }

    /**
     * 交叉表排序项转换
     * 指标项数量为1,以:%为分隔符去掉第一个
     * 指标项数量大于1,以:%为分隔符截取第一个拼接到排序项最后以_分割
     */
    private Map<String, String> crosstabOrderBuiler( SqlBuilder sqlbuilder ) {
        //指标项
        List<String> indexList = sqlbuilder.getIndexList();
        //字段与描述对应关系
        Map<String, String> fieldAndDescMap = sqlbuilder.getFieldAndDescMap();
        //字段与别名对应关系
        Map<String, String> fieldAndAliasMap = sqlbuilder.getFieldAndAliasMap();
        //交叉表排序条件
        Map<String, String> crosstabMap = sqlbuilder.getCrosstabByMap();


        //转换后的交叉表排序条件
        Map<String, String> resultMap = Maps.newLinkedHashMap();
        if (Objects.nonNull(indexList) && Objects.nonNull(crosstabMap) && !crosstabMap.isEmpty()) {
            int indexSize = indexList.size();

            for (Map.Entry<String, String> entry : crosstabMap.entrySet()) {
                String key = entry.getKey();
                if (key.contains(COMPARE_SPLIT_CHAR)) {
                    List<String> fieldList = Lists.newArrayList(key.split(COMPARE_SPLIT_CHAR));
                    if (!fieldList.isEmpty()) {
                        String firstField = fieldList.get(0);
                        fieldList.remove(firstField);
                        String field = fieldList.stream().collect(Collectors.joining(COMPARE_SPLIT_CHAR));
                        if (indexSize > 1) {
                            String fieldName = findKeyByValue(firstField, fieldAndDescMap);
                            if (!Strings.isNullOrEmpty(fieldName)) {
                                String fieldAlias = fieldAndAliasMap.get(fieldName);
                                resultMap.put(field.concat("_").concat(fieldAlias), entry.getValue());
                            }
                        }
                        if (indexSize == 1) {
                            resultMap.put(field, entry.getValue());
                        }
                    }
                } else {
                    String fieldName = findKeyByValue(key, fieldAndDescMap);
                    resultMap.put(fieldAndAliasMap.get(fieldName), entry.getValue());
                }
            }

        }
        return resultMap;
    }

    /**
     * 根据value找key
     */
    private String findKeyByValue( String value, Map<String, String> map ) {
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (value.equals(entry.getValue())) {
                return entry.getKey();
            }
        }
        return "";
    }

    /**
     * 自定义字段表达式
     */
    private String customFieldFilter( SqlBuilder sqlbuilder ) {
        String and = " and ";
        StringBuilder filterFormula = new StringBuilder();
        List<String> filterFormulaList = sqlbuilder.getFilterFormulaList();
        if (Objects.nonNull(filterFormulaList) && !filterFormulaList.isEmpty()) {
            filterFormula.append(filterFormulaList.stream().collect(Collectors.joining(and)));
        }
        return filterFormula.toString();
    }

    /**
     * 完整sql拼接
     */
    public SparkSqlCondition toSparkSql( SqlBuilder sqlbuilder ) {
        SparkSqlCondition sparkSqlCondition = new SparkSqlCondition();
        StringBuilder sqlBuilder = new StringBuilder();
        //select
        sqlBuilder.append(selectBuilder(sqlbuilder));
        sqlBuilder.append(tableBuilder(sqlbuilder));
        //where
        String where = whereBuilder(sqlbuilder);
        //group
        String group = groupBuilder(sqlbuilder);
        //compare
        List<String> compare = compareBuilder(sqlbuilder);
        //order by
        String orderBy = orderBuilder(sqlbuilder);
        //交叉表排序项
        Map<String, String> crosstabMap = crosstabOrderBuiler(sqlbuilder);

        if (!Strings.isNullOrEmpty(where)) {
            sqlBuilder.append(where);
        }
        if (!Strings.isNullOrEmpty(group)) {
            sqlBuilder.append(group);
        }
        if (!Strings.isNullOrEmpty(orderBy)) {
            sqlBuilder.append(orderBy);
        }

        if (compare != null && !compare.isEmpty()) {
            sparkSqlCondition.setCompareList(compare);
        }

        sparkSqlCondition.setIndexList(sqlbuilder.getIndexList());
        sparkSqlCondition.setSelectList(sqlbuilder.getSelectFieldAliasList());
        sparkSqlCondition.setSelectSql(sqlBuilder.toString());
        //spark配置信息
        sparkSqlCondition.setSparkConfig(sqlbuilder.getSparkConfig());
        //分组字段
        sparkSqlCondition.setGroupList(sqlbuilder.getGroupList());
        sparkSqlCondition.setFieldAndDescMap(sqlbuilder.getFieldAndDescMap());
        sparkSqlCondition.setFieldAndAliasMap(sqlbuilder.getFieldAndAliasMap());
        sparkSqlCondition.setQueryType(sqlbuilder.getQueryType());
        sparkSqlCondition.setSparkAggMap(sqlbuilder.getSparkAggMap());
        sparkSqlCondition.setLimit(sqlbuilder.getLimit());
        sparkSqlCondition.setQueryPoint(sqlbuilder.getQueryPoint());
        sparkSqlCondition.setOrderByMap(orderByAliasMap);
        sparkSqlCondition.setCrosstabByMap(crosstabMap);
        sparkSqlCondition.setFilterCustomFieldList(sqlbuilder.getFilterCustomFieldList());
        sparkSqlCondition.setDelFilterField(sqlbuilder.getDelFilterField());
        sparkSqlCondition.setFilterFormula(customFieldFilter(sqlbuilder));
        return sparkSqlCondition;
    }
}

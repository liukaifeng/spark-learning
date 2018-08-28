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


import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.livy.client.ext.model.v1.Constant.COMPARE_SPLIT_CHAR;


/**
 * 查询sql构造
 *
 * @author Created by 刘凯峰
 * @date 2018-03-30 09-30
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
    private String selectBuilder( SqlBuilder sqlbuilder, SqlType sqlType ) {
        StringBuilder stringBuilder = new StringBuilder(SELECT);
        List<String> selectSqlList = Lists.newArrayList();
        //主体SQL查询项
        if (sqlType == SqlType.SQL_MAIN) {
            selectSqlList = sqlbuilder.getSelectSqlList();
        }
        //同环比SQL查询项
        if (sqlType == SqlType.SQL_QOQ) {
            selectSqlList = sqlbuilder.getSelectQoqSqlList();
        }
        int size = selectSqlList.size();

        if (size > 0) {
            selectSqlList.stream().distinct().forEach(select -> {
                stringBuilder.append(select).append(",");
            });
            return stringBuilder.substring(0, stringBuilder.length() - 1);
        }
        return "";
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
    private String whereBuilder( SqlBuilder sqlbuilder, SqlType sqlType ) {
        StringBuilder stringBuilder = new StringBuilder(WHERE);
        List<String> whereList = Lists.newArrayList();
        if (sqlType == SqlType.SQL_MAIN) {
            whereList = sqlbuilder.getWhereSqlList();
        }
        if (sqlType == SqlType.SQL_QOQ) {
            whereList = sqlbuilder.getWhereQoqSqlList();
        }
        int size = whereList.size();
        String and = " and ";

        //过滤项and分割
        if (size > 0) {
            stringBuilder.append(whereList.stream().distinct().collect(Collectors.joining(and)));
        }

        if (!stringBuilder.toString().equals(WHERE)) {
            return stringBuilder.toString();
        }
        return "";
    }

    /**
     * 构造sql的分组条件
     */
    private String groupBuilder( SqlBuilder sqlBuilder, SqlType sqlType ) {
        List<String> groupSqlList = Lists.newArrayList();
        if (sqlType == SqlType.SQL_MAIN) {
            groupSqlList = sqlBuilder.getGroupSqlList();
        }
        if (sqlType == SqlType.SQL_QOQ) {
            groupSqlList = sqlBuilder.getGroupQoqSqlList();
        }
        if (Objects.nonNull(groupSqlList) && !groupSqlList.isEmpty()) {
            return GROUP_BY + groupSqlList.stream().map(s -> "`".concat(s).concat("`")).collect(Collectors.joining(","));
        }
        return "";
    }

    /**
     * 对比条件别名
     */
    private List<String> compareBuilder( SqlBuilder sqlbuilder ) {
        return sqlbuilder.getCompareFieldList();
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

        if (orderByMap != null && !orderByMap.isEmpty()) {
            for (Map.Entry<String, String> entry : orderByMap.entrySet()) {
                orderByAliasMap.put(entry.getKey(), entry.getValue());
                if (sqlbuilder.getQueryPoint() == 2) {
                    sort = Constant.SORT_DESC.equals(entry.getValue().toUpperCase()) ? Constant.SORT_ASC : Constant.SORT_DESC;
                } else {
                    sort = entry.getValue();
                }
                stringBuilder.append("`").append(entry.getKey()).append("` ").append(sort).append(dotStr);
            }
        } else {
            sort = Constant.SORT_DESC;
            //指标字段
            List<String> indexList = sqlbuilder.getIndexList();
            String fieldAliasName = "";

            if (indexList != null && !indexList.isEmpty()) {
                //字段别名
                fieldAliasName = indexList.get(0);
            }
            if (!Strings.isNullOrEmpty(fieldAliasName)) {

                stringBuilder.append("`").append(fieldAliasName).append("` ").append(sort).append(dotStr);
                orderByAliasMap.put(fieldAliasName, sort);
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
                            String fieldAlias = findKeyByValue(firstField, fieldAndDescMap);
                            if (!Strings.isNullOrEmpty(fieldAlias)) {
                                resultMap.put(field.concat("_").concat(fieldAlias), entry.getValue());
                            }
                        }
                        if (indexSize == 1) {
                            resultMap.put(field, entry.getValue());
                        }
                    }
                } else {
                    //字段别名
                    String fieldName = findKeyByValue(key, fieldAndDescMap);
                    resultMap.put(fieldName, entry.getValue());
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
        //对比条件
        List<String> compare = compareBuilder(sqlbuilder);

        //交叉表排序项
        Map<String, String> crosstabMap = crosstabOrderBuiler(sqlbuilder);

        //order by
        String orderBy = orderBuilder(sqlbuilder);

        sparkSqlCondition.setIndexList(sqlbuilder.getIndexList());
        sparkSqlCondition.setSelectList(sqlbuilder.getSelectFieldAliasList());
        sparkSqlCondition.setSelectSql(toSql(sqlbuilder, SqlType.SQL_MAIN));
        sparkSqlCondition.setSelectQoqSql(toSql(sqlbuilder, SqlType.SQL_QOQ));
        //spark配置信息
        sparkSqlCondition.setSparkConfig(sqlbuilder.getSparkConfigMap());
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
        sparkSqlCondition.setDataSourceType(sqlbuilder.getDataSourceType());
        sparkSqlCondition.setKeyspace(sqlbuilder.getKeyspace());
        sparkSqlCondition.setTable(sqlbuilder.getTable());
        sparkSqlCondition.setPage(sqlbuilder.getPage());
        sparkSqlCondition.setQoqList(sqlbuilder.getQoqList());
        if (compare != null && !compare.isEmpty()) {
            sparkSqlCondition.setCompareList(compare);
        }
//        if (Strings.isNullOrEmpty(where)) {
//            sparkSqlCondition.setCassandraFilter("1=1");
//
//        } else {
//            sparkSqlCondition.setCassandraFilter(where.replace(WHERE, ""));
//        }
        return sparkSqlCondition;
    }

    private String toSql( SqlBuilder sqlbuilder, SqlType sqlType ) {
        //主体SQL
        StringBuilder sqlBuilder = new StringBuilder();
        //select
        String select = selectBuilder(sqlbuilder, sqlType);
        if (Strings.isNullOrEmpty(select)) {
            return select;
        }
        sqlBuilder.append(select);
        sqlBuilder.append(tableBuilder(sqlbuilder));
        //Where
        String where = whereBuilder(sqlbuilder, sqlType);
        //group
        String group = groupBuilder(sqlbuilder,sqlType);

        if (!Strings.isNullOrEmpty(where)) {
            sqlBuilder.append(where);
        }
        if (!Strings.isNullOrEmpty(group)) {
            sqlBuilder.append(group);
        }
        return sqlBuilder.toString();
    }

    private enum SqlType {
        /**
         * 主体SQL
         */
        SQL_MAIN,
        /**
         * 同环比SQL
         */
        SQL_QOQ
    }
}

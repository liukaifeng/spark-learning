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
package org.apache.livy.client.ext.model.v2;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.livy.client.ext.model.v1.Constant.*;
import static org.apache.livy.client.ext.model.v1.Constant.DataFieldType.*;
import static org.apache.livy.client.ext.model.v1.Constant.DateType.*;
import static org.apache.livy.client.ext.model.v1.Constant.FunctionType.*;
import static org.apache.livy.client.ext.model.v1.Constant.LogicalOperator.*;

/**
 * sql所需条件拼接
 *
 * @author Created by 刘凯峰
 * @date 2018-06-19 13-07
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class SqlBuilder extends BaseBuilder {

    //region 私有字段及属性
    /**
     * select选项集合
     */
    private final List<String> selectSqlList = Lists.newArrayList();

    /**
     * select 同环比选项集合
     */
    private final List<String> selectQoqSqlList = Lists.newArrayList();

    /**
     * where条件集合
     */
    private final List<String> whereSqlList = Lists.newArrayList();

    /**
     * where同环比条件集合
     */
    private final List<String> whereQoqSqlList = Lists.newArrayList();

    /**
     * 自定义字段作为筛选字段
     */
    private final List<String> filterCustomFieldList = Lists.newArrayList();

    /**
     * 自定义字段筛选表达式
     */
    private final List<String> filterFormulaList = Lists.newArrayList();

    /**
     * 收集需要分组的字段
     */
    private final List<String> groupSqlList = Lists.newArrayList();

    /**
     * 同环比SQL分组字段
     */
    private List<String> groupQoqSqlList = Lists.newArrayList();

    /**
     * 收集维度字段
     */
    private List<String> groupList = Lists.newArrayList();

    /**
     * 指标字段
     */
    private final List<String> indexList = Lists.newArrayList();

    /**
     * 查询项的原始字段名
     */
    private final List<String> selectFieldList = Lists.newArrayList();

    /**
     * 查询项的别名
     */
    private final List<String> selectFieldAliasList = Lists.newArrayList();

    /**
     * 对比字段
     */
    private final List<String> compareFieldList = Lists.newArrayList();

    /**
     * spark 配置信息
     */
    private Map<String, String> sparkConfigMap = Maps.newLinkedHashMap();
    /**
     * 聚合信息
     */
    private final Map<String, List<String>> sparkAggMap = Maps.newLinkedHashMap();

    /**
     * 别名与中文名称对应关系
     */
    private final Map<String, String> fieldAliasAndDescMap = Maps.newLinkedHashMap();



    /**
     * 别名与字段对应关系(解决相同字段问题)
     */
    private final Map<String, String> aliasAndFieldMap = Maps.newLinkedHashMap();

    /**
     * 字段与表达式对应关系
     */
    private final Map<String, Integer> fieldAndFormulaTypeMap = Maps.newLinkedHashMap();

    /**
     * 字段与类型对应关系
     */
    private final Map<String, String> fieldAndTypeMap = Maps.newLinkedHashMap();

    /**
     * 排序字段与升降序对应关系
     */
    private final Map<String, String> orderByMap = Maps.newLinkedHashMap();
    /**
     * 交叉表排序
     */
    private final Map<String, String> crosstabByMap = Maps.newLinkedHashMap();

    /**
     * 聚合字段与别名对应关系
     */
    private final Map<String, String> aggFieldAliasMap = Maps.newLinkedHashMap();

    /**
     * 数据表名
     */
    private String tableName;

    /**
     * 结果集中是否清除自定义字段列
     */
    private Boolean delFilterField = false;
    /**
     * 同环比条件
     */
    private List<QoqDTO> qoqList = Lists.newArrayList();
    //endregion

    //region 构造函数

    /**
     * 构造函数
     */
    public SqlBuilder( BiReportBuildInDTO biReportBuildInDTO ) {
        this.setQueryPoint(biReportBuildInDTO.getQueryPoint());
        this.setQueryType(biReportBuildInDTO.getQueryType());
        this.setLimit(biReportBuildInDTO.getLimit());
        this.setPage(biReportBuildInDTO.getPage());
        this.setDataSourceType(biReportBuildInDTO.getDataSourceType());
        this.setKeyspace(biReportBuildInDTO.getDbName());
        this.setTable(biReportBuildInDTO.getTbName());
        if (Objects.isNull(biReportBuildInDTO.getSparkConfig()) || biReportBuildInDTO.getSparkConfig().isEmpty()) {
            this.sparkConfigMap = Maps.newHashMap();
        } else {
            this.sparkConfigMap = biReportBuildInDTO.getSparkConfig();
        }

        //数据库与表名
        tableBuilder(biReportBuildInDTO.getDbName(), biReportBuildInDTO.getTbName());
        //select
        selectSqlBuilder(biReportBuildInDTO);
        //where
        whereSqlBuilder(biReportBuildInDTO.getFilterCondition());
        //index
        sparkAggBuilder(biReportBuildInDTO.getIndexCondition());
        //orderBy
        orderBySqlBuilder(biReportBuildInDTO);
        //自定义字段作为筛选项
        customFieldHandle(biReportBuildInDTO.getFilterCondition());

        qoqHandle(biReportBuildInDTO.getIndexCondition());
    }
    //endregion

    //region sql_db_table

    /**
     * 数据库及表名拼接
     */
    private void tableBuilder( String dbName, String tbName ) {
        this.tableName = dbName.concat(".").concat(tbName);
    }
    //endregion

    //region sql_select

    /**
     * 查询项拼接
     */
    private void selectSqlBuilder( BiReportBuildInDTO biReportBuildInDTO ) {
        //维度条件-拼成group条件
        List<DimensionConditionBean> dimensionConditionBeanList = biReportBuildInDTO.getDimensionCondition();

        //对比条件
        List<CompareConditionBean> compareConditionList = biReportBuildInDTO.getCompareCondition();

        //指标条件-拼成select条件
        List<IndexConditionBean> indexConditionBeanList = biReportBuildInDTO.getIndexCondition();

        //遍历维度条件
        if (Objects.nonNull(dimensionConditionBeanList) && !dimensionConditionBeanList.isEmpty()) {
            dimensionConditionBeanList.forEach(dimension -> {
                //收集字段与数据类型关系
                fieldAndTypeMapBuilder(dimension.getFieldName(), dimension.getDataType(), dimension.getIsBuildAggregated());
                //拼接查询项
                String fieldAlias = selectBuilder(dimension.getFieldName(), dimension.getFieldAliasName(),
                        dimension.getDataType(), FUNC_GROUP.getCode(),
                        dimension.getFieldFormula(), dimension.getIsBuildAggregated(), dimension.getGranularity(), false);
                //收集字段名与中文名映射
                fieldAliasAndDescMapBuilder(fieldAlias, dimension.getFieldDescription(), dimension.getAliasName());

                if (!containAggFunc(dimension.getFieldFormula(), dimension.getIsBuildAggregated())) {
                    //分组字段收集
                    groupSqlList.add(fieldAlias);
                }
                //维度字段收集
                groupList.add(fieldAlias);
            });
        }

        //遍历对比条件
        if (Objects.nonNull(compareConditionList) && !compareConditionList.isEmpty()) {
            compareConditionList.forEach(compare -> {
                //收集字段与数据类型关系
                fieldAndTypeMapBuilder(compare.getFieldName(), compare.getDataType(), compare.getIsBuildAggregated());
                //拼接查询项
                String fieldAlias = selectBuilder(compare.getFieldName(), compare.getFieldAliasName(),
                        compare.getDataType(), FUNC_COMPARE.getCode(),
                        compare.getFieldFormula(), compare.getIsBuildAggregated(), compare.getGranularity(), false);
                //收集字段名与中文名映射
                fieldAliasAndDescMapBuilder(fieldAlias, compare.getFieldDescription(), compare.getAliasName());

                if (!containAggFunc(compare.getFieldFormula(), compare.getIsBuildAggregated())) {
                    //分组字段收集
                    groupSqlList.add(fieldAlias);
                }
            });
        }

        //遍历指标条件
        indexConditionBeanList.forEach(index -> {
            //收集字段与数据类型关系
            fieldAndTypeMapBuilder(index.getFieldName(), index.getDataType(), index.getIsBuildAggregated());

            boolean qoqFlag = index.getQoqType() > 0;
            //查询项SQL拼接
            String fieldAlias = selectBuilder(index.getFieldName(), index.getFieldAliasName(), index.getDataType(), index.getAggregator(),
                    index.getFieldFormula(), index.getIsBuildAggregated(), index.getGranularity(), qoqFlag);
            //指标项收集
            indexList.add(fieldAlias);
            //构造字段名与中文名映射集合
            fieldAliasAndDescMapBuilder(fieldAlias, index.getFieldDescription(), index.getAliasName());
        });
    }

    /**
     * 拼接查询项
     *
     * @param fieldName      字段名
     * @param dataType       数据类型
     * @param aggregatorType 聚合类型
     * @return 字段别名
     */
    private String selectBuilder( String fieldName, String fieldAliasName, String dataType, String aggregatorType, String formula,
                                  int customAggFlag, String granularity, boolean qoqFlag ) {
        //非同环比查询项
        StringBuilder selectCondition = new StringBuilder();
        //同环比查询项
        StringBuilder selectQoqCondition = new StringBuilder();

        String originFieldName = fieldName;
        String sqlExpression = "";
        if (!Strings.isNullOrEmpty(aggregatorType)) {
            //求和
            aggregatorType = aggregatorType.toLowerCase();
            if (FUNC_SUM.getCode().equals(aggregatorType)) {
                fieldName = customField(fieldName, formula, customAggFlag);

                if (containAggFunc(formula, customAggFlag)) {
                    sqlExpression = String.format(" %s as `%s` ", fieldName, fieldAliasName);
                } else {
                    sqlExpression = String.format(" sum(%s) as `%s` ", fieldName, fieldAliasName);
                }
            }
            //求数量
            if (FUNC_COUNT.getCode().equals(aggregatorType)) {
                fieldName = customField(fieldName, formula, customAggFlag);
                if (containAggFunc(formula, customAggFlag)) {
                    sqlExpression = String.format(" %s as `%s` ", fieldName, fieldAliasName);
                } else {
                    sqlExpression = String.format(" count(%s) as `%s` ", fieldName, fieldAliasName);
                }
            }
            //求平均值
            if (FUNC_AVG.getCode().equals(aggregatorType)) {
                fieldName = customField(fieldName, formula, customAggFlag);
                if (containAggFunc(formula, customAggFlag)) {
                    sqlExpression = String.format(" %s as `%s` ", fieldName, fieldAliasName);
                } else {
                    sqlExpression = String.format(" avg(%s) as `%s` ", fieldName, fieldAliasName);
                }
            }
            //去重计数
            if (FUNC_DISTINCT_COUNT.getCode().equals(aggregatorType)) {
                fieldName = customField(fieldName, formula, customAggFlag);
                if (containAggFunc(formula, customAggFlag)) {
                    sqlExpression = String.format(" %s as `%s` ", fieldName, fieldAliasName);
                } else {
                    sqlExpression = String.format(" count(distinct(%s)) as `%s` ", fieldName, fieldAliasName);
                }
            }
            //对比字段
            if (FUNC_COMPARE.getCode().equals(aggregatorType)) {
                fieldName = dateFieldFormat(fieldName, dataType, granularity);
                fieldName = customField(fieldName, formula, customAggFlag);
                sqlExpression = String.format(" %s as `%s` ", fieldName, fieldAliasName);
                compareFieldList.add(fieldAliasName);
                selectQoqCondition.append(sqlExpression);
            }
            //维度字段
            if (FUNC_GROUP.getCode().equals(aggregatorType)) {
                fieldName = dateFieldFormat(fieldName, dataType, granularity);
                fieldName = customField(fieldName, formula, customAggFlag);
                sqlExpression = String.format(" %s as `%s` ", fieldName, fieldAliasName);
                selectQoqCondition.append(sqlExpression);
            }
        }
        if (Strings.isNullOrEmpty(aggregatorType)) {
            if (Objects.equals(dataType, DataFieldType.DATETIME_TYPE.getType())) {
                sqlExpression = String.format(" date_format(%s,'yyyy-MM-dd') as `%s`", fieldName, fieldName);
            } else {
                fieldName = customField(fieldName, formula, customAggFlag);
                if (customAggFlag == 0) {
                    sqlExpression = String.format(" %s ", fieldName);
                } else {
                    sqlExpression = String.format(" %s as `%s`", fieldName, originFieldName);
                }
            }
        }
        if (!Strings.isNullOrEmpty(sqlExpression) && !qoqFlag) {
            selectCondition.append(sqlExpression);
        } else if (!Strings.isNullOrEmpty(sqlExpression)) {
            selectQoqCondition.append(sqlExpression);
        }
        aliasAndFieldMap.put(fieldAliasName, originFieldName);
        fieldAndFormulaTypeMap.put(originFieldName, customAggFlag);
        if (!Strings.isNullOrEmpty(selectCondition.toString())) {
            selectSqlList.add(selectCondition.toString());
        }
        if (!Strings.isNullOrEmpty(selectQoqCondition.toString())) {
            selectQoqSqlList.add(selectQoqCondition.toString());
        }
        return fieldAliasName;
    }


    /**
     * 收集字段与数据类型关系
     *
     * @param fieldName     字段名
     * @param dataType      数据类型
     * @param customAggFlag 是否是自定义组合字段
     */
    private void fieldAndTypeMapBuilder( String fieldName, String dataType, int customAggFlag ) {
        if (customAggFlag == 0) {
            fieldAndTypeMap.put(fieldName, dataType);
        }
    }

    /**
     * 判断是否是自定义组合字段
     *
     * @param fieldName     字段名
     * @param formula       表达式
     * @param customAggFlag 自定义组合字段标识
     */
    private String customField( String fieldName, String formula, int customAggFlag ) {
        //自定义字段
        if (customAggFlag != 0) {
            fieldName = formula;
        }
        return fieldName;
    }

    /**
     * 表达式是否包含聚合函数
     *
     * @param formula       表达式
     * @param customAggFlag 自定义字段标识
     */
    private boolean containAggFunc( String formula, int customAggFlag ) {
        boolean isContainAggFunc = false;
        if (customAggFlag != 0) {
            for (String func : AGG_FUNCTION) {
                if (formula.toLowerCase().contains(func)) {
                    isContainAggFunc = true;
                    break;
                }
            }
        }
        return isContainAggFunc;
    }
    //endregion

    //region sql_where

    /**
     * where 条件构建
     */
    private void whereSqlBuilder( List<FilterConditionBean> filterConditionBeanList ) {
        //构造where条件
        if (filterConditionBeanList != null && !filterConditionBeanList.isEmpty()) {
            filterConditionBeanList.forEach(filter -> {
                //非聚合字段
                if (filter.getIsBuildAggregated() == 0) {
                    String whereStr = whereBuilder(filter);
                    if (!Strings.isNullOrEmpty(whereStr)) {
                        whereSqlList.add(whereStr);
                        //同环比不受日期筛选条件的限制
                        if (!Objects.equals(filter.getDataType(), DataFieldType.DATETIME_TYPE.getType())) {
                            whereQoqSqlList.add(whereStr);
                        }
                    }
                }
            });
        }
    }

    /**
     * where sql条件拼接
     */
    private String whereBuilder( FilterConditionBean filterCondition ) {
        StringBuilder whereCondition = new StringBuilder();
        //数据类型
        String dataType = filterCondition.getDataType();
        //筛选值
        List<String> values = filterCondition.getFieldValue();
        //字段名
        String fieldName = filterCondition.getFieldName();
        //日期粒度（日周月年季）
        String granularity = filterCondition.getGranularity();
        //逻辑运算符标识
        String aggregator = filterCondition.getAggregator();
        //默认精确匹配值
        String equalsValue = "";
        //默认范围值
        String rangValue = "";
        int valuesSize = filterCondition.getFieldValue().size();
        //字符串
        if (Objects.equals(dataType, STRING_TYPE.getType())) {
            if (valuesSize == 1) {
                equalsValue = String.format("%s='%s'", fieldName, values.get(0));
            }
            if (valuesSize > 1) {
                rangValue = values.stream().map(item -> String.format("'%s'", item)).collect(Collectors.joining(","));
            }
        }
        //日期类型
        if (Objects.equals(dataType, DataFieldType.DATETIME_TYPE.getType())) {
            List<String> times = timeConvert(values, granularity);
            int timesSize = times.size();
            if (!times.isEmpty()) {
                if (timesSize == 1) {
                    equalsValue = String.format("date_format(%s,'yyyy-MM-dd')='%s'", fieldName, times.get(0));
                }
                if (timesSize > 1) {
                    equalsValue = String.format(" date_format(%s,'yyyy-MM-dd')>='%s'", fieldName, times.get(0))
                            .concat(String.format(" and date_format(%s,'yyyy-MM-dd') <'%s'", fieldName, times.get(1)));
                }
            }
        }
        if ((Objects.equals(dataType, DECIMAL_TYPE.getType()) ||
                Objects.equals(dataType, INT_TYPE.getType())) &&
                Objects.nonNull(values) && (values.size() > 0)) {
            if (Objects.equals(aggregator, LOGICAL_BETWEEN.getCode())) {
                whereCondition.append(fieldName).append(String.format(" between %s and %s", values.get(0), values.get(1)));
            }
            if (Objects.equals(aggregator, LOGICAL_EQUAL.getCode())) {
                whereCondition.append(fieldName).append(String.format(" =%s", values.get(0)));
            }
            if (Objects.equals(aggregator, LOGICAL_NOT_EQUAL.getCode())) {
                whereCondition.append(fieldName).append(String.format(" !=%s", values.get(0)));
            }
            if (Objects.equals(aggregator, LOGICAL_GT.getCode())) {
                whereCondition.append(fieldName).append(String.format(" >%s", values.get(0)));
            }
            if (Objects.equals(aggregator, LOGICAL_GTE.getCode())) {
                whereCondition.append(fieldName).append(String.format(" >=%s", values.get(0)));
            }
            if (Objects.equals(aggregator, LOGICAL_LT.getCode())) {
                whereCondition.append(fieldName).append(String.format(" <%s", values.get(0)));
            }
            if (Objects.equals(aggregator, LOGICAL_LTE.getCode())) {
                whereCondition.append(fieldName).append(String.format(" <=%s", values.get(0)));
            }
            if (Objects.equals(aggregator, LOGICAL_IS_NULL.getCode())) {
                whereCondition.append(fieldName).append(" is null");
            }
            if (Objects.equals(aggregator, LOGICAL_IS_NOT_NULL.getCode())) {
                whereCondition.append(fieldName).append(" is not null");
            }
        }
        if (Objects.equals(aggregator, LOGICAL_LIKE.getCode())) {
            equalsValue = "";
            rangValue = "";
            whereCondition.append(fieldName).append(" like '".concat("%").concat(values.get(0)).concat("%'"));
        }
        if (!Strings.isNullOrEmpty(equalsValue) && !"=".equals(equalsValue)) {
            whereCondition.append(equalsValue);
        }
        if (!Strings.isNullOrEmpty(rangValue)) {
            whereCondition.append(fieldName).append(" in (").append(rangValue).append(")");
        }
        return whereCondition.toString();
    }
    //endregion

    //region sql_orderBy

    /**
     * 排序sql拼接
     */
    private void orderBySqlBuilder( BiReportBuildInDTO biReportBuildInDTO ) {
        //排序条件
        List<SortConditionBean> sortList = biReportBuildInDTO.getSortCondition();

        //按顺序对比条件去掉最后一个,其余加入排序
        if (Objects.nonNull(compareFieldList) && !compareFieldList.isEmpty() && compareFieldList.size() > 1) {
            List<String> compareList = Lists.newArrayList(compareFieldList);
            compareList.remove(compareList.size() - 1);
            compareList.forEach(compare -> orderByBuilder(compare, "", 0));
        } else if (Objects.nonNull(groupList) && !groupList.isEmpty() && (Objects.isNull(sortList) || sortList.isEmpty())) {
            //无对比条件时,维度条件第一个字段加入排序
            orderByBuilder(groupList.get(0), "", 0);
        }
        //排序条件遍历
        if (Objects.nonNull(sortList) && !sortList.isEmpty()) {
            sortList.forEach(sort -> orderByBuilder(sort.getFieldAliasName(), sort.getSortFlag(), sort.getSortType()));
        }
    }

    /**
     * 排序sql拼接
     *
     * @param fieldAliasName 字段名
     * @param sortFlag       升降序标识
     * @param sortType       排序类型（0-默认;1-交叉表排序）
     */
    private void orderByBuilder( String fieldAliasName, String sortFlag, int sortType ) {
        String sort = sortFlag;
        if (sortType == 0) {
            if (Strings.isNullOrEmpty(sort)) {
                sort = Constant.SORT_ASC;
            }

            if (!Strings.isNullOrEmpty(fieldAliasName)) {
                orderByMap.put(fieldAliasName, sort);
            }
        }
        if (sortType == 1) {
            crosstabByMap.put(fieldAliasName, sort);
        }
    }

    //endregion

    //region spark_agg

    /**
     * 聚合字段收集
     */
    private void sparkAggBuilder( List<IndexConditionBean> indexList ) {
        List<String> sumList = Lists.newArrayList();
        List<String> avgList = Lists.newArrayList();
        List<String> countList = Lists.newArrayList();
        List<String> disCountList = Lists.newArrayList();

        indexList.forEach(index -> {
            String fieldAliasName = index.getFieldAliasName();
            if (FUNC_SUM.getCode().equals(index.getAggregator())) {
                sumList.add(fieldAliasName);
                sparkAggMap.put(index.getAggregator(), sumList);
            }
            if (FUNC_COUNT.getCode().equals(index.getAggregator())) {
                countList.add(fieldAliasName);
                sparkAggMap.put(index.getAggregator(), countList);
            }
            if (FUNC_AVG.getCode().equals(index.getAggregator())) {
                avgList.add(fieldAliasName);
                sparkAggMap.put(index.getAggregator(), avgList);
            }
            if (FUNC_DISTINCT_COUNT.getCode().equals(index.getAggregator())) {
                disCountList.add(fieldAliasName);
                sparkAggMap.put(index.getAggregator(), disCountList);
            }
            if (!Strings.isNullOrEmpty(fieldAliasName)) {
                aggFieldAliasMap.put(index.getFieldName(), fieldAliasName);
            }
        });
    }
    //endregion

    //region 自定义字段作为筛选项

    /**
     * 自定义字段作为筛选项处理
     * 1、在维度条件、对比条件或指标条件中包含筛选项，记录该筛选项的别名
     * 2、在维度条件、对比条件和指标条件中不包含筛选项：
     * select 中加入筛选项
     * group by 中加入筛选项
     */
    private void customFieldHandle( List<FilterConditionBean> filterConditionList ) {
        if (Objects.nonNull(filterConditionList) && !filterConditionList.isEmpty()) {
            StringBuilder selectBuild = new StringBuilder();
            StringBuilder whereBuilder = new StringBuilder();

            //筛选条件遍历
            filterConditionList.forEach(filter -> {
                //如果是自定义字段
                if (filter.getIsBuildAggregated() > 0) {
                    String fieldName = filter.getFieldName();
                    String fieldAliasName = fieldName;

                    //维度条件、对比条件、指标条件中包含筛选项
                    if (aliasAndFieldMap.values().contains(fieldName)) {
                        fieldAliasName = findKeyByValue(fieldName, aliasAndFieldMap);
                    } else {
                        fieldAliasName = filter.getFieldAliasName();
                        //维度条件、对比条件、指标条件中不包含筛选项
                        delFilterField = true;
                        List<String> values = filter.getFieldValue();
                        if (Objects.nonNull(values) && !values.isEmpty()) {
                            selectBuild.append(String.format("%s as `%s`", filter.getFieldFormula(), fieldAliasName));
                        }
                        //分组字段不为空，将筛选字段添加到分组中
                        if (!groupSqlList.isEmpty()) {
                            groupSqlList.add(fieldAliasName);
                        }
                        filterCustomFieldList.add(fieldAliasName);
                    }
                    //表达式包含聚合函数
                    if (containAggFunc(filter.getFieldFormula(), filter.getIsBuildAggregated())) {
                        filter.setFieldName(fieldAliasName);
                        whereBuilder.append(whereBuilder(filter));
                    } else {
                        //表达式不包含聚合函数，表达式本身直接作为筛选条件拼接到where
                        filter.setFieldName(filter.getFieldFormula());
                        whereSqlList.add(whereBuilder(filter));
                    }
                }
            });
            if (!Strings.isNullOrEmpty(whereBuilder.toString())) {
                filterFormulaList.add(whereBuilder.toString());
            }
            if (!Strings.isNullOrEmpty(selectBuild.toString())) {
                selectSqlList.add(selectBuild.toString());
            }
        }
    }
    //endregion

    //region 同环比条件处理

    /**
     * 同环比条件处理
     */
    private void qoqHandle( List<IndexConditionBean> indexCondition ) {
        if (Objects.nonNull(indexCondition)) {
            List<String> whereList = Lists.newArrayList();
            for (IndexConditionBean index : indexCondition) {
                if (index.getQoqType() > 0) {
                    QoqDTO qoq = convert2QoqDTO(index.getQoqConditionBean());
                    String qoqFieldName = qoq.getFieldName();
                    String qoqFieldAliasName=qoq.getFieldAliasName();
                    if (aliasAndFieldMap.values().contains(qoqFieldName)) {
                        qoqFieldAliasName=findKeyByValue(qoqFieldName,aliasAndFieldMap);
                    }

                    if (!DATE_WEEK.getCode().equals(qoq.getGranularity())) {
                        //对应日期格式化
                        qoqFieldName = dateFieldFormat(qoqFieldName, DataFieldType.DATETIME_TYPE.getType(), qoq.getGranularity());
                        //添加到查询项
                        selectQoqSqlList.add(String.format(" %s as `%s` ", qoqFieldName, qoqFieldAliasName));
                        //添加到where条件中
                        whereList.add(String.format(" %s IN ('%s', '%s')", qoqFieldName, qoq.getQoqRadixTime(), qoq.getQoqReducedTime()));
                    } else {
                        String radixTimeYear = qoq.getQoqRadixTime().split("-")[0];
                        String radixTimeWeek = qoq.getQoqRadixTime().split("-")[1];
                        String reducedTimeYear = qoq.getQoqReducedTime().split("-")[0];
                        String reducedTimeWeek = qoq.getQoqReducedTime().split("-")[1];

                        qoq.setQoqRadixTime(radixTimeYear.concat("-").concat(String.valueOf(Integer.valueOf(radixTimeWeek))));
                        qoq.setQoqReducedTime(reducedTimeYear.concat("-").concat(String.valueOf(Integer.valueOf(reducedTimeWeek))));

                        String inWhere = String.format("'%s','%s'", qoq.getQoqRadixTime(), qoq.getQoqReducedTime());
                        String where = String.format("DATE_FORMAT(%s, 'y') || '-' || WEEKOFYEAR(%s) in (%s)", qoqFieldName, qoqFieldName, inWhere);

                        //添加到查询项
                        selectQoqSqlList.add(String.format(" DATE_FORMAT(%s, 'y') || '-' || WEEKOFYEAR(%s) AS `%s` ", qoqFieldName, qoqFieldName, qoqFieldAliasName));
                        //添加到where条件中
                        whereList.add(where);
                    }

                    qoq.setDelQoqField(true);
                    qoq.setQoqTimeAliasName(qoqFieldAliasName);
                    qoq.setQoqIndexAliasName(index.getFieldAliasName());
                    qoqList.add(qoq);
                    //分组字段不为空，将筛选字段添加到分组中
                    if (!groupSqlList.contains(qoqFieldAliasName)) {
                        groupQoqSqlList.addAll(groupSqlList);
                        groupQoqSqlList.add(qoqFieldAliasName);
                    }
                }
            }
            groupQoqSqlList = groupQoqSqlList.stream().distinct().collect(Collectors.toList());
            if (whereList.size() > 0) {
                String whereStr = whereList.stream().collect(Collectors.joining(" or "));
                whereQoqSqlList.add(String.format("(%s)", whereStr));
            }
        }
    }

    /**
     * 同环比入参条件转换
     */
    private QoqDTO convert2QoqDTO( QoqConditionBean qoqConditionBean ) {
        QoqDTO qoqDTO = new QoqDTO();
        qoqDTO.setFieldName(qoqConditionBean.getFieldName());
        qoqDTO.setFieldDescription(qoqConditionBean.getFieldDescription());
        qoqDTO.setGranularity(qoqConditionBean.getGranularity());
        qoqDTO.setQoqResultType(qoqConditionBean.getQoqResultType());
        qoqDTO.setQoqRadixTime(qoqConditionBean.getQoqRadixTime());
        qoqDTO.setQoqReducedTime(qoqConditionBean.getQoqReducedTime());
        return qoqDTO;
    }
    //endregion

    //region get方法

    /**
     * 查询项别名
     */
    public List<String> getSelectFieldAliasList() {
        List<String> selectFieldAliasList = Lists.newArrayList();
        if (!aliasAndFieldMap.isEmpty()) {
            selectFieldAliasList = new ArrayList<>(aliasAndFieldMap.keySet());
        }
        return selectFieldAliasList;
    }
    //endregion

    //region 内部工具类

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
     * 字段别名与中文对应关系
     *
     * @param fieldAliasName 字段别名
     * @param fieldDesc      字段描述（中文描述）
     * @param fieldAlias     自定义字段描述（中文别名）
     */
    private void fieldAliasAndDescMapBuilder( String fieldAliasName, String fieldDesc, String fieldAlias ) {
        //优先取别名
        if (!Strings.isNullOrEmpty(fieldAliasName)) {
            if (!Strings.isNullOrEmpty(fieldAlias)) {
                fieldAliasAndDescMap.put(fieldAliasName, fieldAlias);
            } else if (!Strings.isNullOrEmpty(fieldDesc)) {
                fieldAliasAndDescMap.put(fieldAliasName, fieldDesc);
            } else {
                fieldAliasAndDescMap.put(fieldAliasName, fieldAliasName);
            }
        }
    }

    /**
     * 日期类型字段转换成相应的表达式
     *
     * @param fieldName     字段名
     * @param fieldDataType 字段类型
     * @param granularity   日期粒度（日周月年）
     */
    private String dateFieldFormat( String fieldName, String fieldDataType, String granularity ) {
        String dateFieldFormula = fieldName;
        if (fieldDataType.equals(DataFieldType.DATETIME_TYPE.getType())) {
            String dateFormat = getDateFormat(granularity);
            if (!Strings.isNullOrEmpty(dateFormat)) {
                dateFieldFormula = String.format("date_format(%s,'%s')", fieldName, dateFormat);
            }
            if (DATE_WEEK.getCode().equals(granularity)) {
                dateFieldFormula = weekFormula.replace("%s", fieldName);
            }
            if (DATE_SEASON.getCode().equals(granularity)) {
                dateFieldFormula = seasonFormula.replace("%s", fieldName);
            }
        }
        return dateFieldFormula;
    }

    /**
     * 获取日期格式
     *
     * @param granularity 日期粒度
     */
    private String getDateFormat( String granularity ) {
        String dateFormat = "";
        //日
        if (DATE_DAY.getCode().equals(granularity) || DATE_UD.getCode().equals(granularity)) {
            dateFormat = DateUtils.DAY_OF_DATE_FRM;
        }
        //周
        if (DATE_WEEK.getCode().equals(granularity)) {
            dateFormat = DateUtils.DAY_OF_DATE_FRM;
        }
        //月
        if (DATE_MONTH.getCode().equals(granularity)) {
            dateFormat = DateUtils.MONTH_OF_DATE_FRM;
        }
        //年
        if (DATE_YEAR.getCode().equals(granularity)) {
            dateFormat = DateUtils.YEAR_OF_DATE_FRM;
        }
        return dateFormat;
    }


    /**
     * 根据日周月年对时间做不同处理
     */
    private List<String> timeConvert( List<String> values, String granularity ) {
        List<String> timeConvertResult = Lists.newArrayList();
        if (Strings.isNullOrEmpty(granularity)) {
            return timeConvertResult;
        }
        String dateFormat = getDateFormat(granularity);
        if (!Strings.isNullOrEmpty(dateFormat)) {
            for (String value : values) {
                if (!Strings.isNullOrEmpty(value)) {
                    if (!Strings.isNullOrEmpty(dateFormat)) {
                        String time = DateUtils.convertTimeToString(value, dateFormat);
                        timeConvertResult.add(time);
                    } else {
                        timeConvertResult.add(value);
                    }
                }
            }
        }
        return timeConvertResult;
    }

    /**
     * 根据字段生成别名
     *
     * @param prefix    前缀
     * @param fieldName 字段名
     */
//    private String fieldAlias( String prefix, String fieldName ) {
//        long count = aliasAndFieldMap.values().stream().filter(value -> value.equals(fieldName)).count();
//        return ALIAS_SPLIT_PREFIX.concat(prefix).concat(ALIAS_SPLIT_SUFFIX).concat(fieldName).concat("_").concat(String.valueOf(count));
//    }
    //endregion
}

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
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.regex.Pattern.compile;
import static org.apache.livy.client.ext.model.Constant.*;
import static org.apache.livy.client.ext.model.Constant.DataFieldType.*;
import static org.apache.livy.client.ext.model.Constant.DateType.DATE_SEASON;
import static org.apache.livy.client.ext.model.Constant.DateType.DATE_WEEK;
import static org.apache.livy.client.ext.model.Constant.FunctionType.*;
import static org.apache.livy.client.ext.model.Constant.LogicalOperator.*;


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
     * 聚合信息
     */
    private final Map<String, List<String>> sparkAggMap = Maps.newLinkedHashMap();


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
     * 别名与中文名称对应关系
     */
    private final Map<String, String> fieldAliasAndDescMap = Maps.newLinkedHashMap();
    /**
     * 同环比字段别名与表达式的映射关系
     */
    private final Map<String, String> fieldAliasAndFormulaMap = Maps.newLinkedHashMap();

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

    /**
     * cassandra 过滤条件
     */
    private String cassandraFilter = "";
    /**
     * 集团编号
     */
    private String groupCode = "group_code";

    //表别名前缀
    private final String tableAliasPrefix = "tb_";
    //默认表别名
    private final String tableAliasDefault = "tb_1";
    //表别名初始值
    private int tableAliasInitValue = 1;
    //默认占位符
    private final String DEFAULT_PLACEHOLDER = SymbolType.SYMBOL_POUND_KEY.getCode();
    private final String SYMBOL_DOT = SymbolType.SYMBOL_DOT.getCode();

    private List<String> qoqSqlList = Lists.newArrayList();

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
        this.setSessionId(biReportBuildInDTO.getSessionId());
        this.setTracId(biReportBuildInDTO.getTracId());
        this.setKuduMaster(biReportBuildInDTO.getKuduMaster());
        this.setHiveJdbcConfig(biReportBuildInDTO.getHiveJdbcConfig());
        if (Objects.nonNull(biReportBuildInDTO.getSparkConfig()) && !biReportBuildInDTO.getSparkConfig().isEmpty()) {
            this.setSparkConfigMap(biReportBuildInDTO.getSparkConfig());
        }
        if (Objects.nonNull(biReportBuildInDTO.getDimensionCondition()) && biReportBuildInDTO.getDimensionCondition().size() > 0) {
            this.setDimensionIsEmpty(false);
        }
        this.setMongoConfigMap(biReportBuildInDTO.getMongoConfig());


        //数据库与表名
        tableBuilder(biReportBuildInDTO);


        selectSqlBuilder(biReportBuildInDTO);
//        //where
        whereSqlBuilder(biReportBuildInDTO.getFilterCondition());
//        //index
//        sparkAggBuilder(biReportBuildInDTO.getIndexCondition());
//        //orderBy
//        orderBySqlBuilder(biReportBuildInDTO);
//        //自定义字段作为筛选项
//        customFieldHandle(biReportBuildInDTO.getFilterCondition());
//
        qoqHandle(biReportBuildInDTO.getIndexCondition());
    }
    //endregion

    //region sql_db_table

    /**
     * 数据库及表名拼接
     */
    private void tableBuilder( BiReportBuildInDTO biReportBuildInDTO ) {
        this.tableName = biReportBuildInDTO.getDbName().replace("impala::", "").concat(".").concat(biReportBuildInDTO.getTbName());
    }
    //endregion

    //region sql_select

    /**
     * 遍历维度条件、对比条件、指标条件
     */
    private void selectSqlBuilder( BiReportBuildInDTO biReportBuildInDTO ) {
        //维度条件-拼成group条件
        List<DimensionConditionBean> dimensionConditionBeanList = biReportBuildInDTO.getDimensionCondition();
        //对比条件
        List<CompareConditionBean> compareConditionList = biReportBuildInDTO.getCompareCondition();
        //指标条件-拼成select条件
        List<IndexConditionBean> indexConditionBeanList = biReportBuildInDTO.getIndexCondition();
        int queryType = biReportBuildInDTO.getQueryType();
        //遍历维度条件
        if (Objects.nonNull(dimensionConditionBeanList) && !dimensionConditionBeanList.isEmpty()) {
            dimensionConditionBeanList.forEach(dimension -> {
                //收集字段与数据类型关系
                fieldAndTypeMapBuilder(dimension.getFieldName(), dimension.getDataType(), dimension.getIsBuildAggregated());
                //待处理查询项
                SelectOptionDTO selectOptionDTO = convert2SelectOptionDTO(dimension);
                selectOptionDTO.setAggregator(FUNC_GROUP.getCode());
                selectOptionDTO.setQueryType(queryType);
                //拼接查询项
                String fieldAlias = selectBuilder(selectOptionDTO);
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
                //待处理查询项
                SelectOptionDTO selectOptionDTO = convert2SelectOptionDTO(compare);
                selectOptionDTO.setAggregator(FUNC_COMPARE.getCode());
                selectOptionDTO.setQueryType(queryType);
                //拼接查询项
                String fieldAlias = selectBuilder(selectOptionDTO);
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
            //待处理查询项
            SelectOptionDTO selectOptionDTO = convert2SelectOptionDTO(index);
            selectOptionDTO.setAggregator(index.getAggregator());
            selectOptionDTO.setQueryType(queryType);
            selectOptionDTO.setQoqFlag(index.getQoqType() > 0);
            //查询项SQL拼接
            String fieldAlias = selectBuilder(selectOptionDTO);
            //指标项收集
            indexList.add(fieldAlias);
            //构造字段名与中文名映射集合
            fieldAliasAndDescMapBuilder(fieldAlias, index.getFieldDescription(), index.getAliasName());
        });
    }


    private String generateTableAlias() {
        return tableAliasPrefix.concat(String.valueOf(++tableAliasInitValue));
    }


    /**
     * 查询项对象转换
     *
     * @param baseConditionBean 查询项父类对象
     * @return org.apache.livy.client.ext.model.SelectOptionDTO
     * @author 刘凯峰
     * @date 2019/1/21 10:42
     */
    private SelectOptionDTO convert2SelectOptionDTO( BaseConditionBean baseConditionBean ) {
        SelectOptionDTO selectOptionDTO = new SelectOptionDTO();
        try {
            BeanUtils.copyProperties(selectOptionDTO, baseConditionBean);
        } catch (IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
        selectOptionDTO.setFieldName(DEFAULT_PLACEHOLDER + selectOptionDTO.getFieldName());
        return selectOptionDTO;
    }

    /**
     * 拼接查询项
     *
     * @param selectOptionDTO 查询项对象
     * @return 字段别名
     */
    private String selectBuilder( SelectOptionDTO selectOptionDTO ) {
        //非同环比查询项
        StringBuilder selectCondition = new StringBuilder();
        //字段名
        String fieldName = selectOptionDTO.getFieldName();
        //原始字段名
        String originFieldName = fieldName;
        //字段别名
        String fieldAliasName = selectOptionDTO.getFieldAliasName();
        //聚合类型
        String aggregatorType = selectOptionDTO.getAggregator();
        //表达式
        String formula = selectOptionDTO.getFieldFormula();
        //自定义聚合字段标识
        int customAggFlag = selectOptionDTO.getIsBuildAggregated();
        //原始数据类型
        String originDataType = selectOptionDTO.getOriginDataType();
        //转换数据类型
        String targetDataType = selectOptionDTO.getDataType();
        //sql 表达式
        String sqlExpression = "";

        if (!Strings.isNullOrEmpty(aggregatorType)) {
            //求和
            aggregatorType = aggregatorType.toLowerCase();
            if (FUNC_SUM.getCode().equals(aggregatorType)) {
                fieldName = customField(fieldName, formula, customAggFlag, selectOptionDTO.getGranularity(), targetDataType);

                if (containAggFunc(formula, customAggFlag)) {
                    sqlExpression = String.format(" %s as `%s` ", fieldName, fieldAliasName);
                } else {
                    sqlExpression = String.format(" sum(CAST(%s AS DOUBLE)) as `%s` ", fieldName, fieldAliasName);
                }
            }
            //求数量
            if (FUNC_COUNT.getCode().equals(aggregatorType)) {
                fieldName = customField(fieldName, formula, customAggFlag, selectOptionDTO.getGranularity(), targetDataType);
                if (containAggFunc(formula, customAggFlag)) {
                    sqlExpression = String.format(" %s as `%s` ", fieldName, fieldAliasName);
                } else {
                    sqlExpression = String.format(" count(%s) as `%s` ", fieldName, fieldAliasName);
                }
            }
            //求平均值
            if (FUNC_AVG.getCode().equals(aggregatorType)) {
                fieldName = customField(fieldName, formula, customAggFlag, selectOptionDTO.getGranularity(), targetDataType);
                if (containAggFunc(formula, customAggFlag)) {
                    sqlExpression = String.format(" %s as `%s` ", fieldName, fieldAliasName);
                } else {
                    sqlExpression = String.format(" avg(CAST(%s AS DOUBLE)) as `%s` ", fieldName, fieldAliasName);
                }
            }
            //去重计数
            if (FUNC_DISTINCT_COUNT.getCode().equals(aggregatorType)) {
                fieldName = customField(fieldName, formula, customAggFlag, selectOptionDTO.getGranularity(), targetDataType);
                if (containAggFunc(formula, customAggFlag)) {
                    sqlExpression = String.format(" %s as `%s` ", fieldName, fieldAliasName);
                } else {
                    sqlExpression = String.format(" count(distinct(%s)) as `%s` ", fieldName, fieldAliasName);
                }
            }
            //对比字段
            if (FUNC_COMPARE.getCode().equals(aggregatorType)) {
                fieldName = customField(fieldName, formula, customAggFlag, selectOptionDTO.getGranularity(), targetDataType);
                sqlExpression = String.format(" cast(%s as String) as `%s` ", fieldName, fieldAliasName);
                compareFieldList.add(fieldAliasName);
                if (!originDataType.equals(DataFieldType.DATETIME_TYPE.getType())) {
                    selectQoqSqlList.add(sqlExpression);
                }
            }
            //维度字段
            if (FUNC_GROUP.getCode().equals(aggregatorType)) {
                fieldName = customField(fieldName, formula, customAggFlag, selectOptionDTO.getGranularity(), targetDataType);
                //数字类型做维度条件，将其转换为字符串类型
                if (originDataType.equals(DECIMAL_TYPE.getType())) {
                    fieldName = String.format(" cast(%s as String)", fieldName);
                }
                sqlExpression = String.format(" %s as `%s` ", fieldName, fieldAliasName);
                if (!originDataType.equals(DataFieldType.DATETIME_TYPE.getType())) {
                    selectQoqSqlList.add(sqlExpression);
                }
            }
        }
        if (Strings.isNullOrEmpty(aggregatorType)) {
            if (Objects.equals(targetDataType, DATETIME_TYPE.getType())) {
                sqlExpression = String.format(" from_timestamp(%s,'yyyy-MM-dd') as `%s`", fieldName, fieldAliasName);
            } else {
                fieldName = customField(fieldName, formula, customAggFlag, selectOptionDTO.getGranularity(), targetDataType);
                //筛选值查询去重并做空值过滤
                if (selectOptionDTO.getQueryType() == 1) {
                    sqlExpression = String.format(" DISTINCT(%s) as `%s`", fieldName, fieldAliasName);
                    if (customAggFlag > 0) {
                        whereSqlList.add(String.format(" %s is not null", formula));
                    } else {
                        whereSqlList.add(String.format(" %s is not null", fieldName));
                    }

                } else {
                    sqlExpression = String.format(" %s as `%s`", fieldName, fieldAliasName);
                }
            }
        }
        if (!Strings.isNullOrEmpty(sqlExpression) && !selectOptionDTO.getQoqFlag()) {
            selectCondition.append(sqlExpression);
        } else if (selectOptionDTO.getQoqFlag()) {
            fieldAliasAndFormulaMap.put(fieldAliasName, sqlExpression);
        }
        aliasAndFieldMap.put(fieldAliasName, originFieldName);
        fieldAndFormulaTypeMap.put(originFieldName, customAggFlag);
        if (!Strings.isNullOrEmpty(selectCondition.toString())) {
            selectSqlList.add(selectCondition.toString());
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
     * 判断是否是自定义组合字段
     *
     * @param fieldName     字段名
     * @param formula       表达式
     * @param customAggFlag 自定义组合字段标识
     */
    private String customField( String fieldName, String formula, int customAggFlag, String granularity, String targetDataType ) {
        if (customAggFlag == 0) {
            if (!Strings.isNullOrEmpty(targetDataType) && Objects.equals(targetDataType, DATETIME_TYPE.getType())) {
                fieldName = getDateFormula(granularity, fieldName);
            }
        }
        //自定义组合字段
        if (customAggFlag == 1) {
            fieldName = formula;
        }
        //对自定义日期计算字段和普通字段，按照指定格式进行格式化
        if (customAggFlag == 2) {
            if (!Strings.isNullOrEmpty(targetDataType) && Objects.equals(targetDataType, DATETIME_TYPE.getType())) {
                fieldName = getDateFormula(granularity, formula);
            } else {
                fieldName = formula;
            }
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
                if (formula.toLowerCase().contains(func.concat("("))) {
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
                        if (!Objects.equals(filter.getDataType(), DATETIME_TYPE.getType())) {
                            whereQoqSqlList.add(whereStr);
                        }
                    }
                }
                if (groupCode.equals(filter.getFieldName())) {
                    if (filter.getFieldValue().size() == 1) {
                        cassandraFilter = String.format(groupCode.concat("=%s"), filter.getFieldValue().get(0));
                    }
                    if (filter.getFieldValue().size() > 1) {
                        cassandraFilter = String.format(groupCode.concat(" in (%s)"), filter.getFieldValue().stream().collect(Collectors.joining(",")));
                    }
                }
            });
        }
    }

    private void whereSqlBean( List<FilterConditionBean> filterList ) {
        if (Objects.nonNull(filterList)) {
            filterList.forEach(filter -> {

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
        //筛选值数量
        int valuesSize = Objects.isNull(filterCondition.getFieldValue()) ? 0 : filterCondition.getFieldValue().size();
        //是否是自定义表达式
        boolean isAgg = filterCondition.getIsBuildAggregated() > 0;
        //字符串
        if (Objects.equals(dataType, STRING_TYPE.getType())) {
            //算子为空，单个值用等于(=)，多个值用包含（in）
            if (Strings.isNullOrEmpty(aggregator)) {
                if (valuesSize == 1) {
                    whereCondition.append(String.format("%s='%s'", fieldName, values.get(0)));
                }
                if (valuesSize > 1) {
                    aggregator = LOGICAL_IN.getCode();
                }
            }
            //not in
            if (Objects.equals(aggregator, LOGICAL_NOT_IN.getCode())) {
                String whereValue = values.stream().map(item -> String.format("'%s'", item)).collect(Collectors.joining(","));
                whereCondition.append(fieldName).append(String.format(" not in (%s)", whereValue));
            }
            //in
            if (Objects.equals(aggregator, LOGICAL_IN.getCode())) {
                String whereValue = values.stream().map(item -> String.format("'%s'", item)).collect(Collectors.joining(","));
                whereCondition.append(fieldName).append(String.format(" in (%s)", whereValue));
            }
            //like
            if (Objects.equals(aggregator, LOGICAL_LIKE.getCode())) {
                whereCondition.append(fieldName).append(" like '".concat("%").concat(values.get(0)).concat("%'"));
            }
        }
        //日期类型
        if (Objects.equals(dataType, DATETIME_TYPE.getType())) {
            //将时间戳解析为对应的时间格式
            List<String> times = timeConvert(values, granularity);
            int timesSize = times.size();
            //日期字段格式化表达式
            String dateExpression;
            //等于表达式
            String equalExpression = " %s ='%s'";
            //范围前包括表达式
            String frontExpression = " %s>='%s'";
            //范围后表达式
            String backExpression = " and %s <'%s'";

            dateExpression = getDateFormula(granularity, fieldName);
            //按每周n筛选，表达式的类型是整形
            if (Objects.equals(granularity, DateType.DATE_EVERY_WEEK.getCode())) {
                //等于表达式
                equalExpression = " %s=%s";
                //范围前包括表达式
                frontExpression = " %s>=%s";
                //范围后表达式
                backExpression = " and %s<%s";
            }
            if (!times.isEmpty()) {
                if (timesSize == 1) {
                    whereCondition.append(String.format(equalExpression, dateExpression, times.get(0)));
                }
                if (timesSize > 1) {
                    String whereValue = String.format(frontExpression, dateExpression, times.get(0))
                            .concat(String.format(backExpression, dateExpression, times.get(1)));
                    whereCondition.append(whereValue);
                }
            }
        }
        //数字类型
        boolean isNumber = (Objects.equals(dataType, DECIMAL_TYPE.getType()) || Objects.equals(dataType, INT_TYPE.getType()));
        //值不为空
        boolean valueNotEmpty = Objects.nonNull(values) && values.size() > 0;

        if (isNumber && valueNotEmpty) {
            if (!"GROUP_CODE".equals(fieldName.toUpperCase())) {
                fieldName = String.format("CAST(%s AS DOUBLE)", fieldName);
            }
            //字段类型为数字,算子为空,默认使用范围条件或等于条件
            if (Strings.isNullOrEmpty(aggregator)) {
                if (values.size() > 1) {
                    whereCondition.append(fieldName).append(String.format(" in (%s)", String.join(",", values)));
                }
                if (values.size() == 1) {
                    whereCondition.append(fieldName).append(String.format(" =%s", values.get(0)));
                }
            }
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
            //大于等于（>=）
            if (Objects.equals(aggregator, LOGICAL_GTE.getCode())) {
                whereCondition.append(fieldName).append(String.format(" >=%s", values.get(0)));
            }
            //小于（<）
            if (Objects.equals(aggregator, LOGICAL_LT.getCode())) {
                whereCondition.append(fieldName).append(String.format(" <%s", values.get(0)));
            }
            //小于等于（<=）
            if (Objects.equals(aggregator, LOGICAL_LTE.getCode())) {
                whereCondition.append(fieldName).append(String.format(" <=%s", values.get(0)));
            }
            //不包含（not in）
            if (Objects.equals(aggregator, LOGICAL_NOT_IN.getCode())) {
                String whereValue = values.stream().map(item -> String.format("%s", item)).collect(Collectors.joining(","));
                whereCondition.append(fieldName).append(String.format(" not in (%s)", whereValue));
            }
            //包含（in）
            if (Objects.equals(aggregator, LOGICAL_IN.getCode())) {
                String whereValue = values.stream().map(item -> String.format("%s", item)).collect(Collectors.joining(","));
                whereCondition.append(fieldName).append(String.format(" in (%s)", whereValue));
            }
        }
        //值为null
        if (Objects.equals(aggregator, LOGICAL_IS_NULL.getCode())) {
            whereCondition.append(fieldName).append(" is null");
        }
        //值为非null
        if (Objects.equals(aggregator, LOGICAL_IS_NOT_NULL.getCode())) {
            whereCondition.append(fieldName).append(" is not null");
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
        //存在对比项并且没有指定排序
        if (Objects.nonNull(compareFieldList) && !compareFieldList.isEmpty()) {
            if (Objects.isNull(sortList) || sortList.isEmpty()) {
                List<String> compareList = Lists.newArrayList(compareFieldList);
                // 按顺序对比条件去掉最后一个,其余加入排序
                if (compareFieldList.size() > 0) {
                    compareList.remove(compareList.size() - 1);
                }
                compareList.forEach(compare -> orderByBuilder(compare, "", 0));
                this.setCompareSortFlag(true);
            }
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
        if (!Strings.isNullOrEmpty(fieldAliasName)) {
            if (sortType == 0) {
                if (Strings.isNullOrEmpty(sort)) {
                    sort = SORT_ASC;
                }
                orderByMap.put(fieldAliasName, sort);
            }
            if (sortType == 1) {
                crosstabByMap.put(fieldAliasName, sort);
            }
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

    private void customFieldHandle( List<FilterConditionBean> filterConditionList ) {
        if (Objects.nonNull(filterConditionList) && !filterConditionList.isEmpty()) {
            //查询项
            StringBuilder selectBuild = new StringBuilder();
            //筛选项
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
                        if (!groupSqlList.contains(fieldAliasName)) {
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
                        String where = whereBuilder(filter);
                        whereSqlList.add(where);
                        whereQoqSqlList.add(where);
                    }
                }
            });
            if (!Strings.isNullOrEmpty(whereBuilder.toString())) {
                filterFormulaList.add(whereBuilder.toString());
            }
            if (!Strings.isNullOrEmpty(selectBuild.toString())) {
                selectSqlList.add(selectBuild.toString());
                selectQoqSqlList.add(selectBuild.toString());
            }
        }
    }
    //endregion

    //region 同环比条件处理

    private void qoqHandle( List<IndexConditionBean> indexCondition ) {
        if (Objects.nonNull(indexCondition)) {
            //同环比指标遍历
            List<IndexConditionBean> qoqIndexList = indexCondition.stream().filter(index -> index.getQoqType() > 0).collect(Collectors.toList());
            for (IndexConditionBean index : qoqIndexList) {
                if (index.getQoqType() > 0) {
                    qoqSqlList.add(generateQoqSql(index));
                }
            }
        }
    }

    /**
     * 生成同环比SQL
     *
     * @param index 指标条件
     * @return java.lang.String
     * @author 刘凯峰
     * @date 2019/3/12 15:42
     */
    private String generateQoqSql( IndexConditionBean index ) {
        //同环比计算指标转换成同环比对象
        QoqDTO qoq = convert2QoqDTO(index.getQoqConditionBean());
        qoq.setQoqType(index.getQoqType());
        //根据同环比计算字段别名获取对应的计算表达式
        String fieldFormula = fieldAliasAndFormulaMap.get(index.getFieldAliasName());
        //生成同环比日期表达式
        String qoqDateFormula = generateQoqDateFormula(qoq);
        selectQoqSqlList.add(qoqDateFormula);
        if (!Strings.isNullOrEmpty(fieldFormula)) {
            selectQoqSqlList.add(fieldFormula);
        }
        //同环比sql拼接，包括on条件
        String qoqSelect = selectQoqSqlList.stream().map(s -> s.replace(DEFAULT_PLACEHOLDER, "")).collect(Collectors.joining(","));
        String qoqGroup = groupSqlList.stream().collect(Collectors.joining(","));
        //生成表别名
        String tableAlias = generateTableAlias();
        //表别名前缀
        String tableAliasPrefix = tableAliasDefault + SYMBOL_DOT;
        //同环比where条件
        String qoqWhere = whereSqlList.stream().collect(Collectors.joining(" and "));
        //同环比主干SQL
        String qoqSelectMainSql = String.format("(SELECT %s FROM %s where %s GROUP BY %s) AS %s ", qoqSelect, this.tableName, qoqWhere, qoqGroup, tableAlias);
        //同环比on条件
        String qoqJoinOn = String.format(" ON from_timestamp (%s, 'yyyy-MM-dd') = %s", tableAliasPrefix.concat(qoq.getFieldName()), tableAlias.concat(SYMBOL_DOT).concat(qoq.getFieldAliasName()));
        //同环比计算字段拆分出表达式和对应的别名
        String[] formulas = fieldFormula.split("as");
        //同环比计算字段表达式
        String calculateFieldFormula = formulas[0].replace(DEFAULT_PLACEHOLDER, tableAliasPrefix);
        //同环比计算表达式，增长值
        String qoqCalculateFormula = String.format("%s - min(COALESCE(%s,0)) as %s", calculateFieldFormula, tableAlias.concat(SYMBOL_DOT).concat(formulas[1]), formulas[1]);
        selectSqlList.add(qoqCalculateFormula);
        return qoqSelectMainSql + qoqJoinOn;
    }

    /**
     * 生成同环比日期表达式
     *
     * @param qoq 同环比日期信息
     * @return java.lang.String
     * @author 刘凯峰
     * @date 2019/3/12 13:49
     */
    private String generateQoqDateFormula( QoqDTO qoq ) {
        String formula = "";
        String alias = findKeyByValue(DEFAULT_PLACEHOLDER + qoq.getFieldName(), aliasAndFieldMap);
        qoq.setFieldAliasName(alias);
        //按天同环比
        if (qoq.getGranularity().equals(DateType.DATE_DAY.getCode())) {
            //同比
            if (qoq.getQoqType() == 1) {
                formula = String.format("years_add(from_timestamp(%s,'yyyy-MM-dd'),-1) as %s", qoq.getFieldName(), alias);
            }
            //环比
            if (qoq.getQoqType() == 2) {
                formula = String.format("days_add(from_timestamp(%s,'yyyy-MM-dd'),-1) as %s", qoq.getFieldName(), alias);
            }
        }
        return formula;
    }

    /**
     * 同环比入参条件转换
     */
    private QoqDTO convert2QoqDTO( QoqConditionBean qoqConditionBean ) {
        QoqDTO qoqDTO = new QoqDTO();
        qoqDTO.setFieldName(qoqConditionBean.getFieldName());
        qoqDTO.setFieldAliasName(qoqConditionBean.getFieldAliasName());
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

    /**
     * 是否是同环比字段
     *
     * @param fieldAliasName 被判断字段别名
     * @return boolean
     * @author 刘凯峰
     * @date 2019/1/8 11:13
     */
    public boolean isQoqField( String fieldAliasName ) {
        boolean qoqFlag = false;
        if (Objects.nonNull(qoqList) && !qoqList.isEmpty()) {
            List<QoqDTO> qoqDTOS = qoqList.parallelStream().filter(qoq -> qoq.getQoqIndexAliasName().equals(fieldAliasName)).collect(Collectors.toList());
            if (Objects.nonNull(qoqDTOS) && !qoqDTOS.isEmpty()) {
                qoqFlag = true;
            }
        }
        return qoqFlag;
    }

    public boolean isFliterItem() {
        return getQueryType() == 1;
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
     * 获取日期格式
     *
     * @param granularity 日期精度
     */
    private String getDateFormat( String granularity ) {
        String dateFormat = DATE_TYPE_FORMAT_MAP.get(granularity);
        //默认精确到秒
        if (Strings.isNullOrEmpty(dateFormat)) {
            dateFormat = DateUtils.SECOND_OF_DATE_FRM;
        }
        return dateFormat;
    }


    /**
     * 获取日期表达式
     *
     * @param granularity 日期精度
     * @param fieldName   字段名
     * @return java.lang.String 返回日期表达式
     * @author 刘凯峰
     * @date 2019/2/28 16:57
     */
    private String getDateFormula( String granularity, String fieldName ) {
        String dateFormula = fieldName;
        String dateFormat = getDateFormat(granularity);
        if (!Strings.isNullOrEmpty(fieldName)) {
            if (!Strings.isNullOrEmpty(dateFormat)) {
                dateFormula = String.format("from_timestamp(%s,'%s')", fieldName, dateFormat);
            }
            //按周的维度进行筛选，使用dayofweek表达式，筛选值需要转换才能使用
            if (Objects.equals(granularity, DATE_WEEK.getCode()) || Objects.equals(granularity, DateType.DATE_MAP_WEEK.getCode())) {
                dateFormula = weekFormula2.replace("%s", fieldName);
            }
            //按季度的维度进行筛选，使用dayofweek表达式，筛选值需要转换才能使用
            if (Objects.equals(granularity, DATE_SEASON.getCode()) || Objects.equals(granularity, DateType.DATE_MAP_SEASON.getCode())) {
                dateFormula = seasonFormula3.replace("%s", fieldName);
            }
            //按每周n筛选，使用everyWeekFormula 表达式
            if (Objects.equals(granularity, DateType.DATE_EVERY_WEEK.getCode())) {
                dateFormula = everyWeekFormula.replace("%s", fieldName);
            }
        }
        return dateFormula;
    }


    /**
     * 根据日周月年对时间做不同处理
     */
    private List<String> timeConvert( List<String> values, String granularity ) {
        List<String> timeConvertResult = Lists.newArrayList();
        if (Strings.isNullOrEmpty(granularity)) {
            return timeConvertResult;
        }
        //每周或月或年等匹配
        boolean everyIsMatch = compile("^every_").matcher(granularity.toLowerCase()).find();
        //以every_开头的标识不做日期转换
        if (everyIsMatch) {
            return values;
        }
        String dateFormat = getDateFormat(granularity);
        if (!Strings.isNullOrEmpty(dateFormat)) {
            for (String value : values) {
                if (!Strings.isNullOrEmpty(value)) {
                    if (!Strings.isNullOrEmpty(dateFormat)) {
                        String time = DateUtils.convertTimeToString(value, dateFormat);
                        //如果格式化后的日期为1970，则使用格式化前的值
                        if (!time.equals(DateUtils.DEFAULT_TIME)) {
                            timeConvertResult.add(time);
                        } else {
                            timeConvertResult.add(value);
                        }
                    } else {
                        timeConvertResult.add(value);
                    }
                }
            }
        }
        return timeConvertResult;
    }


    //endregion
}

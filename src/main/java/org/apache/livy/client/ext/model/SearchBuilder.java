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
import static org.apache.livy.client.ext.model.Constant.PCT_SUFFIX_1;
import static org.apache.livy.client.ext.model.Constant.SymbolType.SYMBOL_POUND_KEY;


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

    /**
     * select 语句拼接
     */
    private String selectBuilder( SqlBuilder sqlbuilder ) {
        StringBuilder stringBuilder = new StringBuilder(SELECT);
        List<String> selectSqlList = Lists.newArrayList();
        selectSqlList = sqlbuilder.getSelectSqlList();
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
    private String whereBuilder( SqlBuilder sqlbuilder ) {
        StringBuilder stringBuilder = new StringBuilder(WHERE);
        List<String> whereList = sqlbuilder.getWhereSqlList();
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
    private String groupBuilder( SqlBuilder sqlBuilder ) {
        List<String> groupSqlList = sqlBuilder.getGroupSqlList();
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
    private SqlSortBean orderBuilder( SqlBuilder sqlbuilder ) {
        //排序条件分隔符
        String dotStr = ",";
        //默认升序
        String sort = Constant.SORT_ASC;

        //sql 排序语句拼接
        StringBuilder stringBuilder = new StringBuilder(ORDER_BY);
        //排序字段与升降序对应关系
        Map<String, String> orderByMap = sqlbuilder.getOrderByMap();
        //百分比字段
        Map<String, String> pctMap = sqlbuilder.getPctMap();

        SqlSortBean sqlSortBean = new SqlSortBean();
        //排序字段不为空
        if (orderByMap != null && !orderByMap.isEmpty()) {
            //遍历排序项
            for (Map.Entry<String, String> entry : orderByMap.entrySet()) {
                String key = entry.getKey();
                if (!pctMap.isEmpty() && pctMap.containsKey(key.concat(PCT_SUFFIX_1))) {
                    key = key.concat(PCT_SUFFIX_1);
                }
                sqlSortBean.setSortFieldAliasName(key);
                //0-全部 1-前几条 2-后几条
                if (sqlbuilder.getQueryPoint() == 2) {
                    sort = Constant.SORT_DESC.equals(entry.getValue().toUpperCase()) ? Constant.SORT_ASC : Constant.SORT_DESC;
                    String resultSort = String.format(" %s `%s` %s", ORDER_BY, key, entry.getValue()).concat(getSortNull(entry.getValue()));
                    sqlSortBean.setSortAfterExpression(resultSort);
                    sqlSortBean.setAfterFlag(true);
                } else {
                    sort = entry.getValue();
                }
                stringBuilder.append("`").append(key).append("` ").append(sort).append(getSortNull(sort)).append(dotStr);
            }
        } else {
            sort = Constant.SORT_DESC;
            //指标字段
            List<String> indexList = sqlbuilder.getIndexList();
            String fieldAliasName = "";
            //同环比字段不参与排序
//            if (Objects.nonNull(indexList) && !indexList.isEmpty()) {
//                indexList = indexList.parallelStream().filter(index -> !sqlbuilder.isQoqField(index)).collect(Collectors.toList());
//                if (Objects.nonNull(indexList) && indexList.size() > 0) {
//                    //字段别名
//                    fieldAliasName = indexList.get(0);
//                }
//            }
            if (!Strings.isNullOrEmpty(fieldAliasName)) {
                sqlSortBean.setSortFieldAliasName(fieldAliasName);
                stringBuilder.append("`").append(fieldAliasName).append("` ").append(getSortNull(sort)).append(dotStr);
            }
        }
        if (Objects.equals(stringBuilder.toString(), ORDER_BY)) {
            return null;
        }
        //sql 排序语句
        String sqlOrderBy = stringBuilder.substring(0, stringBuilder.length() - dotStr.length());
        sqlSortBean.setSortExpression(sqlOrderBy);
        return sqlSortBean;
    }


    /**
     * 排序空值处理，null值始终作为最小值
     * sort 为 DESC 时，语句加上 NULLS LAST
     * sort 为 ASC 时，语句加上 NULLS FIRST
     *
     * @param sort 升降序标识
     * @return java.lang.String
     * @author 刘凯峰
     * @date 2019/1/15 16:52
     */
    private String getSortNull( String sort ) {
        return Objects.isNull(sort) || Constant.SORT_DESC.equals(sort.toUpperCase()) ? " NULLS LAST" : " NULLS FIRST";
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
        Map<String, String> fieldAndDescMap = sqlbuilder.getFieldAliasAndDescMap();

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
                        //多个指标条件
                        if (indexSize > 1) {
                            String fieldAlias = findKeyByValue(firstField, fieldAndDescMap);
                            if (!Strings.isNullOrEmpty(fieldAlias)) {
                                resultMap.put(field.concat("_").concat(fieldAlias), entry.getValue());
                            }
                        }
                        //一个指标条件
                        if (indexSize == 1) {
                            resultMap.put(field, entry.getValue());
                        }
                    }
                } else {
                    //字段别名
                    String fieldName = findKeyByValue(key, fieldAndDescMap);
                    if (!Strings.isNullOrEmpty(fieldName)) {
                        resultMap.put(fieldName, entry.getValue());
                    }
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
        //主SQL的where条件
        String mainSqlWhere = whereBuilder(sqlbuilder);
        sparkSqlCondition.setKuduMaster(sqlbuilder.getKuduMaster());
        sparkSqlCondition.setIndexList(sqlbuilder.getIndexList());
        sparkSqlCondition.setSelectList(sqlbuilder.getSelectFieldAliasList());
        sparkSqlCondition.setSelectSql(toSql(sqlbuilder, mainSqlWhere));
        //spark配置信息
        sparkSqlCondition.setSparkConfig(sqlbuilder.getSparkConfigMap());
        //分组字段
        sparkSqlCondition.setGroupList(sqlbuilder.getGroupList());
        sparkSqlCondition.setLimit((Objects.isNull(sqlbuilder.getLimit()) || sqlbuilder.getLimit() <= 0) ? Constant.DEFAULT_LIMIE.intValue() : sqlbuilder.getLimit());
        sparkSqlCondition.setQueryType(sqlbuilder.getQueryType());
        sparkSqlCondition.setSparkAggMap(sqlbuilder.getSparkAggMap());
        sparkSqlCondition.setQueryPoint(sqlbuilder.getQueryPoint());
        sparkSqlCondition.setCrosstabByMap(crosstabMap);
        sparkSqlCondition.setFilterCustomFieldList(sqlbuilder.getFilterCustomFieldList());
        sparkSqlCondition.setDelFilterField(sqlbuilder.getDelFilterField());
        sparkSqlCondition.setFilterFormula(customFieldFilter(sqlbuilder));
        sparkSqlCondition.setDataSourceType(sqlbuilder.getDataSourceType());
        sparkSqlCondition.setKeyspace(sqlbuilder.getKeyspace());
        sparkSqlCondition.setTable(sqlbuilder.getTable());
        sparkSqlCondition.setPage(sqlbuilder.getPage());
//        sparkSqlCondition.setQoqList(sqlbuilder.getQoqList());
        sparkSqlCondition.setSessionId(sqlbuilder.getSessionId());
        sparkSqlCondition.setTracId(sqlbuilder.getTracId());
        sparkSqlCondition.setCompareSortFlag(sqlbuilder.getCompareSortFlag());
        sparkSqlCondition.setMongoConfigMap(sqlbuilder.getMongoConfigMap());
        sparkSqlCondition.setDimensionIsExists(sqlbuilder.getDimensionIsExists());
        sparkSqlCondition.setHiveJdbcConfig(sqlbuilder.getHiveJdbcConfig());
        sparkSqlCondition.setPctMap(sqlbuilder.getPctMap());
        sparkSqlCondition.setComparePctFlag(sqlbuilder.getComparePctFlag());
        if (compare != null && !compare.isEmpty()) {
            sparkSqlCondition.setCompareList(compare);
        }
        if (sqlbuilder.getDataSourceType() == 1 && !Strings.isNullOrEmpty(sqlbuilder.getCassandraFilter())) {
            sparkSqlCondition.setCassandraFilter(sqlbuilder.getCassandraFilter());
        } else {
            sparkSqlCondition.setCassandraFilter("1=1");
        }
        //交叉表排序条件非空，但是无对比项的情况，需要二次计算
        boolean crossOrderByNonNull = Objects.nonNull(sparkSqlCondition.getCrosstabByMap()) && sparkSqlCondition.getCrosstabByMap().size() > 0;
        boolean compareIsNull = Objects.isNull(sparkSqlCondition.getCompareList()) || sparkSqlCondition.getCompareList().size() <= 0;
        //同环比条件非空
        boolean qoqNonNull = sparkSqlCondition.getQoqList() != null && sparkSqlCondition.getQoqList().size() > 0;
        //自定义字段作为筛选项
        boolean customFieldNonNull = Objects.nonNull(sparkSqlCondition.getFilterCustomFieldList()) && sparkSqlCondition.getFilterCustomFieldList().size() > 0;
        //对比条件非空
        boolean compareNonNull = Objects.nonNull(sparkSqlCondition.getCompareList()) && sparkSqlCondition.getCompareList().size() > 0;
        if ((crossOrderByNonNull && compareIsNull) || customFieldNonNull || compareNonNull) {
            sparkSqlCondition.setSecondaryFlag(true);
        }
        return sparkSqlCondition;
    }

    private String toSql( SqlBuilder sqlbuilder, String where ) {
        //sql 拼接对象
        StringBuilder sqlBuilder = new StringBuilder();
        //查询项
        String select = selectBuilder(sqlbuilder);
        //表名
        String tableName = tableBuilder(sqlbuilder);
        //排序项
        SqlSortBean sqlOrderBean = orderBuilder(sqlbuilder);
        //join sql
        String joinSql = "";

        if (Strings.isNullOrEmpty(select)) {
            return select;
        }
        //如果存在join子句，主干sql需要起别名
        if (Objects.nonNull(sqlbuilder.getSelectJoinSqlList()) && sqlbuilder.getSelectJoinSqlList().size() > 0) {
            select = select.replace(SYMBOL_POUND_KEY.getCode(), "tb_1.");
            tableName = tableName + " as tb_1";
            joinSql = " left join " + sqlbuilder.getSelectJoinSqlList().stream().collect(Collectors.joining(" left join "));
        }
        //拼接查询项
        sqlBuilder.append(select);
        //拼接表名
        sqlBuilder.append(tableName);
        if (!Strings.isNullOrEmpty(joinSql)) {
            //拼接join查询
            sqlBuilder.append(joinSql);
        }
        //分组项
        String group = groupBuilder(sqlbuilder);
        //拼接where条件
        if (!Strings.isNullOrEmpty(where)) {
            sqlBuilder.append(where);
        }
        //拼接分组条件
        if (!Strings.isNullOrEmpty(group)) {
            sqlBuilder.append(group);
        }
        //完整SQL语句
        String sql = sqlBuilder.toString();
        //结果集条数限制
        String limit = getLimit(sqlbuilder);
        //非交叉表排序，拼接为SQL
        if (!Strings.isNullOrEmpty(sqlBuilder.toString())
                && Objects.nonNull(sqlOrderBean)
                && sqlbuilder.getCrosstabByMap().isEmpty()
                && !sqlbuilder.isFliterItem()) {
            //排序表达式
            String orderBy = sqlOrderBean.getSortExpression();
            //sql语句拼接
            sql = sqlBuilder.append(orderBy).toString().concat(" limit ").concat(limit);
            //对获取后n条数据排序
            if (sqlOrderBean.getAfterFlag()) {
                orderBy = sqlOrderBean.getSortAfterExpression();
                sql = String.format("select * from (%s) as tb %s limit %s", sql, orderBy, Constant.DEFAULT_LIMIE);
            }
        }
        return sql.replace(SYMBOL_POUND_KEY.getCode(), "");
    }

    /**
     * 数据截取数量
     * 1、请求数量小于等于0或大于1500或存在对比项，重置数量为1500条
     * 2、筛选项查询条数限制100
     *
     * @param sqlbuilder 参数
     * @return java.lang.String
     * @author 刘凯峰
     * @date 2019/1/16 10:03
     */
    private String getLimit( SqlBuilder sqlbuilder ) {
        //数据条数限制
        long limit = 0L;
        //存在对比项，条数限制1500，最后结果集根据指定条数截取
        boolean compareFlag = Objects.nonNull(sqlbuilder.getCompareFieldList()) && sqlbuilder.getCompareFieldList().size() > 0;
        //请求数据量小于等于0或大于1500条，使用默认条数限制
        if (sqlbuilder.getLimit() <= 0 || sqlbuilder.getLimit() > Constant.DEFAULT_LIMIE || compareFlag) {
            limit = Constant.DEFAULT_LIMIE;
        } else {
            limit = sqlbuilder.getLimit();
        }
        //筛选项查询条数限制
        if (sqlbuilder.isFliterItem()) {
            limit = Constant.FILTER_ITEM_LIMIE;
        }
        //数据导出条数限制,请求条数小于等于0或大于10000条时，限制条数10000条
        if (sqlbuilder.getQueryType() == 2) {
            if (sqlbuilder.getLimit() <= 0 || sqlbuilder.getLimit() > Constant.EXPORT_LIMIE || Objects.equals(sqlbuilder.getLimit(), Constant.DEFAULT_LIMIE)) {
                limit = Constant.EXPORT_LIMIE;
            }
        }
        //对比项不为空，重置请求条数
        if (!compareFlag) {
            sqlbuilder.setLimit(Math.toIntExact(limit));
        }
        return String.valueOf(limit);
    }

}

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.livy.client.ext.model.Constant.SORT_ASC;
import static org.apache.livy.client.ext.model.Constant.SORT_DESC;

/**
 * @package: cn.com.tcsl.loongboss.bigscreen.biz.corp.service.impl
 * @project-name: tcsl-loongboss-parent
 * @description: sql条件构造
 * @author: Created by 刘凯峰
 * @create-datetime: 2018-03-30 11-10
 */
public class SqlConditionBuilder {
    private static Logger logger = LoggerFactory.getLogger(SqlConditionBuilder.class);

    private final List<String> whereList = Lists.newArrayList();
    private final List<String> selectSqlList = Lists.newArrayList();//拼装好的select选项
    private final List<String> selectList = Lists.newArrayList();//select字段
    private final List<String> indexList = Lists.newArrayList();//指标项
    private final List<String> ascList = Lists.newArrayList();//升序排序字段
    private final List<String> descList = Lists.newArrayList();//降序排序字段
    private final List<String> sumList = Lists.newArrayList();//求和字段
    private final List<String> countList = Lists.newArrayList();//统计数量字段
    private final List<String> avgList = Lists.newArrayList();//计算平均值字段
    private final List<String> compareList = Lists.newArrayList();//对比字段
    private final List<String> groupList = Lists.newArrayList();//分组字段
    private final List<String> dimensionList = Lists.newArrayList();//维度字段
    private final Map<String, String> sparkConfigMap = Maps.newConcurrentMap();
    private final Map<String, String> fieldMap = Maps.newConcurrentMap();// 字段与中文名称对应关系


    private String tableName;
    private String limit;
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
    //查询项拼接
    public SqlConditionBuilder selectSqlBuilder( String fieldName, String dataType, String aggregatorType, String aliasName) {
        logger.info("【SqlConditionBuilder】-【selectSqlBuilder】进入-字段名【{}}】,数据类型【{}】,聚合类型【{}】", fieldName, dataType, aggregatorType);
        StringBuilder selectCondition = new StringBuilder();
        if (!Strings.isNullOrEmpty(aggregatorType)) {
            aggregatorType = aggregatorType.toLowerCase();
            if ("sum".equals(aggregatorType)) {
                selectCondition.append(fieldName);
                sumList.add(fieldName);
            }
            if ("count".equals(aggregatorType)) {
                selectCondition.append(fieldName);
                countList.add(fieldName);
            }
            if ("avg".equals(aggregatorType)) {
                selectCondition.append(fieldName);
                avgList.add(fieldName);
            }
        }
        if (Strings.isNullOrEmpty(aggregatorType)) {
            if (Objects.equals(dataType, Constant.DataFieldType.DATETIME_TYPE.getType())) {
                selectCondition.append(String.format(" date_format(%s,'yyyy-MM-dd') as %s", fieldName, fieldName));
            } else {
                selectCondition.append(String.format(" %s ", fieldName));
            }
        }
        if (!Strings.isNullOrEmpty(selectCondition.toString())) {
            selectSqlList.add(selectCondition.toString());
        }
        logger.info("【SqlConditionBuilder】-【selectSqlBuilder】结束");
        return this;
    }

    //维度条件构建
    public SqlConditionBuilder dimensionBuilder( String fieldName) {
        dimensionList.add(fieldName);
        return this;
    }

    //数据表名
    public SqlConditionBuilder tableBuilder( String dbName, String tbName) {
        this.tableName = dbName.concat(".").concat(tbName);
        return this;
    }

    //where 条件构建
    public SqlConditionBuilder whereBuilder( String fieldName, List<String> values, String dataType) {
        StringBuilder whereCondition = new StringBuilder();
        //默认精确匹配值
        String equalsValue = "";
        //默认范围值
        String rangValue = "";
        int valuesSize = values.size();
        //字符串
        if (Objects.equals(dataType, Constant.DataFieldType.STRING_TYPE.getType())) {
            if (valuesSize == 1) {
                equalsValue = String.format("='%s'", values.get(0));
            }
            if (valuesSize > 1) {
                rangValue = values.parallelStream().map(item -> String.format("'%s'", item)).collect(Collectors.joining(","));
            }
        }
        //日期类型
        if (Objects.equals(dataType, Constant.DataFieldType.DATETIME_TYPE.getType())) {
            if (valuesSize == 1) {
                equalsValue = String.format("=date_format('%s','yyyy-MM-dd')", values.get(0));
            }
            if (valuesSize > 1) {
                equalsValue = String.format(">=date_format('%s','yyyy-MM-dd')", values.get(0))
                        .concat(String.format(" and %s <=date_format('%s','yyyy-MM-dd')", fieldName, values.get(1)));
            }
        }
        if (!Strings.isNullOrEmpty(equalsValue) && !"=".equals(equalsValue)) {
            whereCondition.append(fieldName).append(equalsValue);
        }
        if (!Strings.isNullOrEmpty(rangValue)) {
            whereCondition.append(fieldName).append(" in (").append(rangValue).append(")");
        }
        if (!Strings.isNullOrEmpty(whereCondition.toString())) {
            whereList.add(whereCondition.toString());
        }
        return this;
    }

    //spark 配置信息
    public SqlConditionBuilder sparkConfigBuilder( Map<String, String> sprakConfigMaps) {
        if (sprakConfigMaps != null && !sprakConfigMaps.isEmpty()) {
            for (Map.Entry<String, String> map : sprakConfigMaps.entrySet()) {
                this.sparkConfigMap.put(map.getKey().replace('_', '.'), map.getValue());
            }
        }
        return this;
    }

    //分组条件构建
    public SqlConditionBuilder groupSparkBuilder( String fieldName) {
        groupList.add(fieldName);
        return this;
    }

    //对比条件构建
    public SqlConditionBuilder compareBuilder( String fieldName) {
        compareList.add(fieldName);
        return this;
    }

    //排序条件构建
    public SqlConditionBuilder orderByBuilder( String fieldName, String sortFlag) {
        if (Strings.isNullOrEmpty(sortFlag)) {
            ascList.add(fieldName);
        } else {
            if (SORT_ASC.equals(sortFlag.toUpperCase())) {
                ascList.add(fieldName);
            }
            if (SORT_DESC.equals(sortFlag.toUpperCase())) {
                descList.add(fieldName);
            }
        }
        return this;
    }

    //指标条件构建
    public SqlConditionBuilder indexBuilder( String fieldName) {
        indexList.add(fieldName);
        return this;
    }

    //数量限制条件构建
    public SqlConditionBuilder limitBuilder( int limitValue) {
        StringBuilder limitBuilder = new StringBuilder();
        if (limitValue > 0) {
            limitBuilder.append(limitValue);
        } else {
            limitBuilder.append("1000");
        }
        limit = limitBuilder.toString();
        return this;
    }

    //字段名与别名映射关系
    public SqlConditionBuilder fieldMapBuilder( String fieldName, String fieldDesc, String fieldAlias) {
        //优先取别名
        if (!Strings.isNullOrEmpty(fieldName)) {
            if (!Strings.isNullOrEmpty(fieldAlias)) {
                fieldMap.put(fieldName, fieldAlias);
            } else if (!Strings.isNullOrEmpty(fieldDesc)) {
                fieldMap.put(fieldName, fieldDesc);
            } else {
                fieldMap.put(fieldName, fieldName);
            }
        }
        return this;
    }


    public SqlConditionBuilder queryTypeBuilder( int queryType) {
        this.queryType = queryType;
        return this;
    }


    public List<String> getWhereList() {
        return whereList;
    }

    public List<String> getSelectSqlList() {
        return selectSqlList;
    }

    public List<String> getSelectList() {
        return selectList;
    }

    public List<String> getIndexList() {
        return indexList;
    }


    public List<String> getCompareList() {
        return compareList;
    }


    public List<String> getGroupByList() {
        return groupList;
    }

    public String getTableName() {
        return tableName;
    }

    public Map<String, String> getSparkConfig() {
        return sparkConfigMap;
    }

    public String getLimit() {
        return limit;
    }

    public List<String> getAscList() {
        return ascList;
    }

    public List<String> getDescList() {
        return descList;
    }

    public List<String> getSumList() {
        return sumList;
    }

    public List<String> getCountList() {
        return countList;
    }

    public List<String> getAvgList() {
        return avgList;
    }

    public List<String> getDimensionList() {
        return dimensionList;
    }

    public Map<String, String> getFieldMap() {
        return fieldMap;
    }

    public int getQueryType() {
        return queryType;
    }

    public int getQueryPoint() {
        return queryPoint;
    }

    public void setQueryPoint( int queryPoint ) {
        this.queryPoint = queryPoint;
    }

}

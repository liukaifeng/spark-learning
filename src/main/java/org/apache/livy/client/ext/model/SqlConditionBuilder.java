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

/**
 * @package: cn.com.tcsl.loongboss.bigscreen.biz.corp.service.impl
 * @project-name: tcsl-loongboss-parent
 * @description: sql条件构造
 * @author: Created by 刘凯峰
 * @create-datetime: 2018-03-30 11-10
 */
public class SqlConditionBuilder {
    private List<String> whereList = Lists.newArrayList();
    private List<String> selectSqlList = Lists.newArrayList();//拼装好的select选项
    private List<String> selectList = Lists.newArrayList();//select字段
    private List<String> indexList = Lists.newArrayList();//指标项
    private List<String> orderByList = Lists.newArrayList();
    private List<String> sumList = Lists.newArrayList();
    private List<String> compareList = Lists.newArrayList();
    private List<String> groupList = Lists.newArrayList();
    private Map<String, String> sparkConfigMap = Maps.newHashMap();
    private String groupBy;
    private String limit;
    private String tableName;

    public SqlConditionBuilder selectSqlBuilder( String fieldName, String dataType, String aggregatorType, String aliasName ) {
        StringBuilder selectCondition = new StringBuilder();
        StringBuilder sumCondition = new StringBuilder();

        if ("sum".equals(aggregatorType)) {
            selectCondition.append(String.format(" sum(%s) as %s ", fieldName, fieldName));
            sumCondition.append(String.format("%s", fieldName));
        }
        if ("count".equals(aggregatorType)) {
            selectCondition.append(String.format(" count(%s) as %s ", fieldName, fieldName));
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
        if (!Strings.isNullOrEmpty(sumCondition.toString())) {
            sumList.add(sumCondition.toString());
        }
        return this;
    }

    public SqlConditionBuilder selectBuilder( String fieldName ) {
        selectList.add(fieldName);
        return this;
    }

    public SqlConditionBuilder indexBuilder( String fieldName ) {
        indexList.add(fieldName);
        return this;
    }

    public SqlConditionBuilder tableBuilder( String dbName, String tbName ) {
        this.tableName = dbName.concat(".").concat(tbName);
        return this;
    }

    /**
     * @method-name: whereBuilder2
     * @description: where条件构建
     * @author: 刘凯峰
     * @date: 2018/4/8 15:37
     * @param: [fieldName, values, dataType]
     * @return: cn.com.tcsl.loongboss.bigscreen.biz.corp.service.impl.SqlConditionBuilder
     * @version V1.0
     * update-logs:方法变更说明
     * ****************************************************
     * name:
     * date:
     * description:
     * *****************************************************
     */
    public SqlConditionBuilder whereBuilder( String fieldName, List<String> values, String dataType ) {
        StringBuilder whereCondition = new StringBuilder();
        //默认精确匹配值
        String equalsValue = values.size() > 0 ? String.format("=%s", values.get(0)) : "";
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
        if (!Strings.isNullOrEmpty(equalsValue)) {
            whereCondition.append(fieldName).append(equalsValue);
        }
        if (!Strings.isNullOrEmpty(rangValue)) {
            whereCondition.append(fieldName).append(rangValue);
        }
        whereList.add(whereCondition.toString());
        return this;
    }

    public SqlConditionBuilder sparkConfigBuilder( Map<String, String> sprakConfigMaps ) {
        if (sprakConfigMaps != null && !sprakConfigMaps.isEmpty()) {
            for (Map.Entry<String, String> map : sprakConfigMaps.entrySet()) {
                this.sparkConfigMap.put(map.getKey().replace('_', '.'), map.getValue());
            }
        }
        return this;
    }

    /**
     * @method-name: groupBuilder
     * @description: 分组条件构建
     * @author: 刘凯峰
     * @date: 2018/4/8 15:37
     * @param: [fieldNames]
     * @return: cn.com.tcsl.loongboss.bigscreen.biz.corp.service.impl.SqlConditionBuilder
     * @version V1.0
     * update-logs:方法变更说明
     * ****************************************************
     * name:
     * date:
     * description:
     * *****************************************************
     */
    public SqlConditionBuilder groupSqlBuilder( List<String> fieldNames ) {
        groupBy = fieldNames.parallelStream().collect(Collectors.joining(","));
        return this;
    }

    public SqlConditionBuilder groupSparkBuilder( String fieldName ) {
        groupList.add(fieldName);
        return this;
    }

    /**
     * @method-name: compareBuilder
     * @description: 对比条件
     * @author: 刘凯峰
     * @date: 2018/4/16 11:37
     * @param: [fieldName]
     * @return: com.spark.model.SqlConditionBuilder
     * @version V1.0
     * update-logs:方法变更说明
     * ****************************************************
     * name:
     * date:
     * description:
     * *****************************************************
     */
    public SqlConditionBuilder compareBuilder( String fieldName ) {
        compareList.add(fieldName);
        return this;
    }


    /**
     * @method-name: orderByBuilder
     * @description: 排序条件构建
     * @author: 刘凯峰
     * @date: 2018/4/8 15:36
     * @param: [fieldName, sortFlag]
     * @return: cn.com.tcsl.loongboss.bigscreen.biz.corp.service.impl.SqlConditionBuilder
     * @version V1.0
     * update-logs:方法变更说明
     * ****************************************************
     * name:
     * date:
     * description:
     * *****************************************************
     */
    public SqlConditionBuilder orderByBuilder( String fieldName, String sortFlag ) {
        StringBuilder orderByCondition = new StringBuilder();
        orderByCondition.append(fieldName);
        if (Strings.isNullOrEmpty(sortFlag)) {
            orderByCondition.append(" ASC");
        } else {
            orderByCondition.append(" ").append(sortFlag);
        }
        orderByList.add(orderByCondition.toString());
        return this;
    }

    public SqlConditionBuilder limitBuilder( int limitValue ) {
        StringBuilder limitBuilder = new StringBuilder();
        if (limitValue > 0) {
            limitBuilder.append(limitValue);
        } else {
            limitBuilder.append("1000");
        }
        limit = limitBuilder.toString();
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

    public List<String> getSumList() {
        return sumList;

    }

    public List<String> getCompareList() {
        return compareList;

    }

    public String getGroupBy() {
        return groupBy;
    }

    public List<String> getGroupByList() {
        return groupList;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getOrderByList() {
        return orderByList;
    }

    public Map<String, String> getSparkConfig() {
        return sparkConfigMap;
    }

    public String getLimit() {
        return limit;
    }
}

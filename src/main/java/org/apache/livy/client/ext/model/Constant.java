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


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * @package: cn.com.tcsl.loongboss.common.constant
 * @class-name: Constant
 * @description: 常量
 * @author: 刘凯峰
 * @date: 2017/10/19 20:26
 */
public class Constant {

    public static final String SORT_ASC = "ASC";
    public static final String SORT_DESC = "DESC";
    /**
     * 默认返回结果集条数
     */
    public static Long DEFAULT_LIMIE = 1500L;
    /**
     * 筛选项条数限制
     */
    public static int FILTER_ITEM_LIMIE = 100;
    /**
     * 对比反转字段别名
     */
    public static final String PIVOT_ALIAS = "y";

    /**
     * 对比项，值分隔符
     */
    public static final String COMPARE_SPLIT_CHAR = ":%";
    /**
     * 用户自定义函数集合
     */
    public static final Map<Integer, String> SPARK_UDF_MAP = Maps.newLinkedHashMap();
    /**
     * 聚合函数集合
     */
    public static final List<String> AGG_FUNCTION = Lists.newArrayList();

    /**
     * 日期类型与表达式对应关系
     */
    public static final Map<String, String> DATE_TYPE_FORMAT_MAP = Maps.newLinkedHashMap();

    public static String weekFormula = "CASE \n" +
            "WHEN dayofweek(`%s`) = 1 THEN '周一'\n" +
            "WHEN dayofweek(`%s`) = 2 THEN '周二'\n" +
            "WHEN dayofweek(`%s`) = 3 THEN '周三'\n" +
            "WHEN dayofweek(`%s`) = 4 THEN '周四'\n" +
            "WHEN dayofweek(`%s`) = 5 THEN '周五'\n" +
            "WHEN dayofweek(`%s`) = 6 THEN '周六'\n" +
            "WHEN dayofweek(`%s`) = 7 THEN '周日'\n" +
            "END";
    public static String seasonFormula = "CASE\n" +
            "WHEN (MONTH(`%s`) BETWEEN 1 and 3) THEN '第1季度'\n" +
            "WHEN (MONTH(`%s`) BETWEEN 4 and 6) THEN '第2季度'\n" +
            "WHEN (MONTH(`%s`) BETWEEN 7 and 9) THEN '第3季度'\n" +
            "WHEN (MONTH(`%s`) BETWEEN 10 and 12) THEN '第4季度'\n" +
            "END ";

    static {
        for (SparkUdf sparkUdf : SparkUdf.values()) {
            SPARK_UDF_MAP.put(sparkUdf.getCode(), sparkUdf.getFunction());
        }
        //聚合函数名
        for (FunctionType functionType : FunctionType.values()) {
            AGG_FUNCTION.add(functionType.getCode());
        }
        //日期类型与表达式对应关系
        for (DateType dateType : DateType.values()) {
            DATE_TYPE_FORMAT_MAP.put(dateType.getCode(), dateType.getFormat());
        }
    }

    public enum DataFieldType {
        STRING_TYPE("str", "字符串类型"),
        INT_TYPE("int", "整型"),
        DECIMAL_TYPE("double", "浮点型"),
        DATETIME_TYPE("datetime", "时间类型");
        private String type;
        private String desc;

        DataFieldType( String type, String desc ) {
            this.type = type;
            this.desc = desc;
        }

        public String getType() {
            return type;
        }
    }

    /**
     * 日期类型
     */
    public enum DateType {
        DATE_YEAR("year", "yyyy"),
        DATE_SEASON("season", "season"),
        DATE_MONTH("month", "yyyy-MM"),
        DATE_WEEK("week", "yyyy-MM-dd"),
        DATE_DAY("day", "yyyy-MM-dd"),
        DATE_DAY_SECOND("day_second", "yyyy-MM-dd HH:mm:ss"),
        DATE_UD("ud", "yyyy-MM-dd"),
        DATE_EVERY_YEAR("every_year", "MM-dd"),
        DATE_EVERY_MONTH("every_month", "dd"),
        DATE_EVERY_WEEK("every_week", "d"),
        DATE_EVERY_DAY("every_day", "HH:mm:ss");

        private String code;

        public String getFormat() {
            return format;
        }

        private String format;

        DateType( String code, String format ) {
            this.code = code;
            this.format = format;
        }

        public String getCode() {
            return code;
        }


    }

    /**
     * 函数类型
     */
    public enum FunctionType {
        FUNC_SUM("sum"),
        FUNC_COUNT("count"),
        FUNC_AVG("avg"),
        FUNC_DISTINCT_COUNT("dis_count"),
        FUNC_FILTER("filter"),
        FUNC_COMPARE("compare"),
        FUNC_MIN("min"),
        FUNC_MAX("max"),
        FUNC_GROUP("group"),
        FUNC_QOQ("qoq");

        private String code;

        FunctionType( String code ) {
            this.code = code;

        }

        public String getCode() {
            return code;
        }
    }

    /**
     * 逻辑运算符
     */
    public enum LogicalOperator {
        LOGICAL_BETWEEN("between_and"),
        LOGICAL_EQUAL("equal"),
        LOGICAL_NOT_EQUAL("not_equal"),
        LOGICAL_LT("lt"),
        LOGICAL_GT("gt"),
        LOGICAL_LTE("lte"),
        LOGICAL_GTE("gte"),
        LOGICAL_IS_NULL("is_null"),
        LOGICAL_IS_NOT_NULL("is_not_null"),
        LOGICAL_LIKE("like"),
        LOGICAL_NOT_IN("not_in"),
        LOGICAL_IN("in");


        private String code;

        LogicalOperator( String code ) {
            this.code = code;

        }

        public String getCode() {
            return code;
        }
    }

    /**
     * spark 用户自定义函数枚举
     */
    public enum SparkUdf {
        /**
         * 自定义排序字段转换函数
         */
        UDF_ORDER_BY(1, "to_orderby(%s)");
        private int code;
        private String function;

        SparkUdf( int code, String function ) {
            this.code = code;
            this.function = function;
        }

        public String getFunction() {
            return this.function;
        }

        public int getCode() {
            return this.code;
        }
    }
}

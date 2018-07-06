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
    public static final String AGG_SUM = "SUM";
    public static final String AGG_COUNT = "COUNT";
    public static final String AGG_AVG = "AVG";
    public static final String PIVOT_ALIAS = "y";

    public static final String ALIAS_SPLIT_SUFFIX = "_0_";
    public static final String ALIAS_SPLIT_PREFIX = "ljc_";
    public static final String COMPARE_SPLIT_CHAR = ":%";
    public static final Map<Integer, String> SPARK_UDF_MAP = Maps.newLinkedHashMap();

    public static final List<String> AGG_FUNCTION = Lists.newArrayList();

    public static String weekFormula = "CASE \n" +
            "WHEN dayofweek(%s) = 1 THEN '周一'\n" +
            "WHEN dayofweek(%s) = 2 THEN '周二'\n" +
            "WHEN dayofweek(%s) = 3 THEN '周三'\n" +
            "WHEN dayofweek(%s) = 4 THEN '周四'\n" +
            "WHEN dayofweek(%s) = 5 THEN '周五'\n" +
            "WHEN dayofweek(%s) = 6 THEN '周六'\n" +
            "WHEN dayofweek(%s) = 7 THEN '周日'\n" +
            "END";
    public static String seasonFormula = "CASE\n" +
            "WHEN (date_format(settle_biz_date,'M') BETWEEN 1 and 3) THEN '第1季度'\n" +
            "WHEN (date_format(settle_biz_date,'M') BETWEEN 4 and 6) THEN '第2季度'\n" +
            "WHEN (date_format(settle_biz_date,'M') BETWEEN 7 and 9) THEN '第3季度'\n" +
            "WHEN (date_format(settle_biz_date,'M') BETWEEN 10 and 12) THEN '第4季度'\n" +
            "END ";

    static {
        for (SparkUdf sparkUdf : SparkUdf.values()) {
            SPARK_UDF_MAP.put(sparkUdf.getCode(), sparkUdf.getFunction());
        }
        //聚合函数名
        for (FunctionType functionType : FunctionType.values()) {
            AGG_FUNCTION.add(functionType.getCode());
        }
    }

    /**
     * @package: cn.com.tcsl.loongboss.common.constant
     * @class-name: DataType
     * @description: 数据类型
     * @author: 刘凯峰
     * @date: 2018/3/29 16:23
     */
    public enum DataType {
        STRING_TYPE(1, "字符串类型"),
        INT_TYPE(2, "整型"),
        DECIMAL_TYPE(3, "浮点型"),
        DATETIME_TYPE(4, "时间类型");
        private int type;
        private String desc;

        DataType( int type, String desc ) {
            this.type = type;
            this.desc = desc;
        }

        public int getType() {
            return type;
        }

        public String getDesc() {
            return desc;
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

        public String getDesc() {
            return desc;
        }
    }

    /**
     * 日期类型
     */
    public enum DateType {
        DATE_YEAR("year"),
        DATE_SEASON("season"),
        DATE_MONTH("month"),
        DATE_WEEK("week"),
        DATE_DAY("day"),
        DATE_UD("ud");

        private String code;

        DateType( String code ) {
            this.code = code;
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
        FUNC_GROUP("group");

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
        LOGICAL_GTE("gte");

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

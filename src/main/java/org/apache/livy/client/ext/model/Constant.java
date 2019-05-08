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
     * 导出数据条数限制
     */
    public static int EXPORT_LIMIE = 10000;

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
     * 百分比计算字段后缀
     */
    public static final String PCT_SUFFIX_1 = "_pct1";
    /**
     * 百分比计算字段后缀
     */
    public static final String PCT_SUFFIX_2 = "_pct2";
    /**
     * 用户自定义函数集合
     */
    public static final Map<Integer, String> SPARK_UDF_MAP = Maps.newHashMap();
    /**
     * 聚合函数集合
     */
    public static final List<String> AGG_FUNCTION = Lists.newArrayList();

    /**
     * 日期类型与表达式对应关系
     */
    public static final Map<String, String> DATE_TYPE_FORMAT_MAP = Maps.newHashMap();

    /**
     * 周逻辑值与中文映射关系
     */
    public static final Map<String, String> WEEK_CN_MAP = Maps.newHashMap();

    /**
     * 季度逻辑值与中文映射关系
     */
    public static final Map<String, String> SEASON_CN_MAP = Maps.newHashMap();
    /**
     * 算子与表达式的映射关系
     */
    public static final Map<String, String> AGG_FUNCTION_MAP = Maps.newHashMap();
    /**
     * 同环比集合
     */
    public static final List<Integer> QOQ_LIST = Lists.newArrayList();

    /**
     * 日期格式化表达式
     */
    public static String DATE_FORMULA = "UNIX_TIMESTAMP(date_trunc('%s',%s))";
    /**
     * 季度格式化表达式
     */
    public static String DATE_QUARTER_FORMULA = "UNIX_TIMESTAMP(trunc (%s,'%s'))";


    public static String weekFormula2 = "CONCAT(from_timestamp (%s, 'yyyy'),'年第',CAST(WEEKOFYEAR(%s) AS STRING  ),'周')";

    public static String everyWeekFormula = "IF(\n" +
            "    DAYOFWEEK(%s) = 1,\n" +
            "    DAYOFWEEK(%s) + 6,\n" +
            "    DAYOFWEEK(%s) - 1\n" +
            "  )";
    /**
     * 季度格式化表达式
     */
    public static String seasonFormula = " CONCAT(CAST(YEAR(%s) AS STRING),'年第',CAST(QUARTER(%s) AS STRING ), '季度')";

    static {
        for (SparkUdf sparkUdf : SparkUdf.values()) {
            SPARK_UDF_MAP.put(sparkUdf.getCode(), sparkUdf.getFunction());
        }
        //聚合函数名
        for (FunctionType functionType : FunctionType.values()) {
            AGG_FUNCTION.add(functionType.getCode());
            if (!Strings.isNullOrEmpty(functionType.getFormula())) {
                AGG_FUNCTION_MAP.put(functionType.getCode(), functionType.getFormula());
            }
        }
        //日期类型与表达式对应关系
        for (DateType dateType : DateType.values()) {
            DATE_TYPE_FORMAT_MAP.put(dateType.getCode(), dateType.getFormat());
        }
        WEEK_CN_MAP.put("周一", "2");
        WEEK_CN_MAP.put("周二", "3");
        WEEK_CN_MAP.put("周三", "4");
        WEEK_CN_MAP.put("周四", "5");
        WEEK_CN_MAP.put("周五", "6");
        WEEK_CN_MAP.put("周六", "7");
        WEEK_CN_MAP.put("周日", "1");

        SEASON_CN_MAP.put("第1季度", "1");
        SEASON_CN_MAP.put("第2季度", "2");
        SEASON_CN_MAP.put("第3季度", "3");
        SEASON_CN_MAP.put("第4季度", "4");

        QOQ_LIST.add(AdvancedCmpType.ADVANCED_QOQ_1.getCode());
        QOQ_LIST.add(AdvancedCmpType.ADVANCED_QOQ_2.getCode());
        QOQ_LIST.add(AdvancedCmpType.ADVANCED_ROLL_QOQ_2.getCode());
        QOQ_LIST.add(AdvancedCmpType.ADVANCED_ROLL_QOQ_WEEK.getCode());
        QOQ_LIST.add(AdvancedCmpType.ADVANCED_ROLL_QOQ_MONTH.getCode());
        QOQ_LIST.add(AdvancedCmpType.ADVANCED_ROLL_QOQ_YEAR.getCode());
        QOQ_LIST.add(AdvancedCmpType.ADVANCED_ROLL_QOQ_SEASON.getCode());
        QOQ_LIST.add(AdvancedCmpType.ADVANCED_QOQ_CUSTOM.getCode());
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
     * DATE_EVERY_*：权限设置时的条件，筛选值不需要进行日期格式化直接使用
     * <p>
     * DATE_MAP_*：日期钻取时的条件标识，筛选值需要转换才能使用（季度、周）
     */
    public enum DateType {
        DATE_YEAR("year", "year"),
        DATE_SEASON("season", "Q"),
        DATE_MONTH("month", "month"),
        DATE_WEEK("week", "week"),
        DATE_DAY("day", "day"),
        DATE_DAY_SECOND("day_second", "second"),
        DATE_UD("ud", "yyyy-MM-dd"),
        DATE_EVERY_YEAR("every_year", "MM-dd"),
        DATE_EVERY_MONTH("every_month", "dd"),
        DATE_EVERY_WEEK("every_week", "d"),
        DATE_EVERY_DAY("every_day", "HH:mm:ss"),
        DATE_MAP_SEASON("map_season", "Q"),
        DATE_MAP_WEEK("map_week", "week");

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
        FUNC_SUM("sum", "sum(CAST(%s AS DOUBLE)) as `%s`"),
        FUNC_COUNT("count", "count(%s) as `%s`"),
        FUNC_AVG("avg", "avg(CAST(%s AS DOUBLE)) as `%s`"),
        FUNC_DISTINCT_COUNT("dis_count", "count(distinct(%s)) as `%s`"),
        FUNC_FILTER("filter", ""),
        FUNC_COMPARE("compare", ""),
        FUNC_MIN("min", ""),
        FUNC_MAX("max", ""),
        FUNC_GROUP("group", ""),
        FUNC_QOQ("qoq", ""),
        FUNC_PCT("pct", "");

        private String code;
        private String formula;

        FunctionType( String code, String formula ) {
            this.code = code;
            this.formula = formula;
        }

        public String getCode() {
            return code;
        }

        public String getFormula() {
            return formula;
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

    /**
     * 符号枚举
     *
     * @author 刘凯峰
     * @date 2019/3/11 15:08
     */
    public enum SymbolType {
        SYMBOL_DOT(".", "点"),
        SYMBOL_POUND_KEY("#", "井号键");
        private String code;
        private String desc;

        SymbolType( String code, String desc ) {
            this.code = code;
            this.desc = desc;
        }

        public String getDesc() {
            return this.desc;
        }

        public String getCode() {
            return this.code;
        }
    }

    /**
     * 高级计算类型
     *
     * @author 刘凯峰
     * @date 2019/3/13 15:36
     */
    public enum AdvancedCmpType {
        ADVANCED_QOQ_1(1, "同比"),
        ADVANCED_QOQ_2(2, "环比"),
        ADVANCED_QOQ_CUSTOM(6, "自定义"),
        ADVANCED_PCT(7, "百分比"),
        ADVANCED_ROLL_QOQ_2(8, "滚动环比计算"),
        ADVANCED_ROLL_QOQ_WEEK(9, "周滚动同比计算"),
        ADVANCED_ROLL_QOQ_MONTH(10, "月滚动同比计算"),
        ADVANCED_ROLL_QOQ_YEAR(11, "年滚动同比计算"),
        ADVANCED_ROLL_QOQ_SEASON(12, "季度滚动同比计算");

        private int code;
        private String desc;

        AdvancedCmpType( int code, String desc ) {
            this.code = code;
            this.desc = desc;
        }

        public String getDesc() {
            return this.desc;
        }

        public int getCode() {
            return this.code;
        }
    }
}

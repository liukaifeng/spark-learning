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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * <p>
 * 基于jdk1.8 新特性 java.time 的日期格式化处理工具类
 * </p>
 *
 * @author 刘凯峰
 * @date 2018-06-07
 */
public class DateUtils {
    /**
     * 时间格式 年月日
     */
    public final static String DAY_OF_DATE_FRM = "yyyy-MM-dd";

    /**
     * 时间格式 年月日 时分秒
     */
    public final static String SECOND_OF_DATE_FRM = "yyyy-MM-dd HH:mm:ss";

    /**
     * 时间格式 年月日 时分秒 毫秒
     */
    public final static String MILLS_SECOND_OF_DATE_FRM = "yyyy-MM-dd HH:mm:ss:SSS";

    /**
     * 时间格式 年月
     */
    public final static String MONTH_OF_DATE_FRM = "yyyy-MM";
    /**
     * 时间格式 年月
     */
    public final static String YEAR_OF_DATE_FRM = "yyyy";

    /**
     * 转换指定Long型时间戳为指定格式时间
     */
    public static String convertTimeToString(Long time, String format) {
        return dateTimeFormatter(format).format(LocalDateTime.ofInstant(Instant.ofEpochMilli(time), ZoneId.systemDefault()));
    }

    /**
     * 转换指定字符串时间戳为指定格式时间
     */
    public static String convertTimeToString(String time, String format) {
        return convertTimeToString(Long.valueOf(time), format);
    }

    /**
     * 转换指定字符串时间戳为年月日格式（yyyy-MM-dd）
     */
    public static String convertTimeToString(String time) {
        return convertTimeToString(Long.valueOf(time), DAY_OF_DATE_FRM);
    }

    /**
     * 格式化指定时间格式
     */
    private static DateTimeFormatter dateTimeFormatter(String format) {
        return DateTimeFormatter.ofPattern(format);
    }

}

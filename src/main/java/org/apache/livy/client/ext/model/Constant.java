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
}

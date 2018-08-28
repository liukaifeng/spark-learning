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
package org.apache.livy.client.ext.model.v1;

import lombok.Data;

/**
 * @package: cn.com.tcsl.loongboss.bigscreen.api.report.model
 * @class-name: SortConditionBean
 * @description: 排序条件
 * @author: 刘凯峰
 * @date: 2018/3/29 15:19
 */
@Data
public class SortConditionBean extends BaseConditionBean {
    /**
     * 字段ID
     */
    private String fieldId;
    /**
     * 排序标识：desc-降序；asc-升序
     */
    private String sortFlag;

    /**
     * 排序类型 0-默认；1-交叉表排序
     */
    private int sortType;
}

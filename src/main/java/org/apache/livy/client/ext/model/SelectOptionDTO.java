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

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * 查询项实体对象
 *
 * @author 刘凯峰
 * @date 2019-01-21 10-13
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class SelectOptionDTO extends BaseConditionBean {
    /**
     * 聚合代码，eg：SUM
     */
    private String aggregator;

    /**
     * 查询类型
     * 0-默认值（普通查询）
     * 1-筛选项数据查询
     */
    private Integer queryType;
    /**
     * 表别名
     */
    private String tableAlias;
    /**
     * 维度条件为空，对比条件不为空情况下的百分计算使用
     */
    private Boolean dimensionIsEmpty;
    /**
     * 对比项是否为空
     */
    private Boolean compareIsEmpty;
}

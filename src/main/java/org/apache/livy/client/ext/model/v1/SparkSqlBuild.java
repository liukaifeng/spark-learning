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

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

/**
 * 根据条件组装sparksql
 *
 * @author Created by 刘凯峰
 * @date 2018-04-14 11-11
 */
public class SparkSqlBuild {
    private final Logger logger = LoggerFactory.getLogger(SparkSqlBuild.class);

    public SparkSqlCondition buildSqlStatement( String param) {
        logger.info("【SparkSqlBuild::buildSqlStatement】-入参param {}", param);
        BiReportBuildInDTO biReportBuildInDTO = JSONObject.parseObject(param, BiReportBuildInDTO.class);
        //维度条件
        List<DimensionConditionBean> dimensionList = biReportBuildInDTO.getDimensionCondition();
        //对比条件
        List<CompareConditionBean> compareList = biReportBuildInDTO.getCompareCondition();
        //指标条件
        List<IndexConditionBean> indexList = biReportBuildInDTO.getIndexCondition();
        //维度、对比不为空,指标为空,清空对比条件
        if (Objects.nonNull(dimensionList) && !dimensionList.isEmpty() &&
                Objects.nonNull(compareList) && !compareList.isEmpty() &&
                (Objects.isNull(indexList) || indexList.isEmpty())) {
            biReportBuildInDTO.setCompareCondition(Lists.newArrayList());
        }
        SqlBuilder sqlBuilder = new SqlBuilder(biReportBuildInDTO);
        return new SearchBuilder().toSparkSql(sqlBuilder);
    }
}

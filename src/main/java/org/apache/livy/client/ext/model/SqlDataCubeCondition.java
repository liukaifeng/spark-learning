package org.apache.livy.client.ext.model;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.livy.client.ext.model.Constant.FunctionType.*;

/**
 * 来自data cube 的请求参数转换
 *
 * @author 刘凯峰
 * @date 2019-02-25 11-30
 */
public class SqlDataCubeCondition {

    /**
     * data cube 请求参数解析
     *
     * @param param 请求参数
     * @return org.apache.livy.client.ext.model.SparkSqlCondition
     * @author 刘凯峰
     * @date 2019/2/25 13:32
     */
    public SparkSqlCondition parseDataCubeCondition( String param ) {
        BiReportBuildInDTO reportBuildInDTO = JSONObject.parseObject(param, BiReportBuildInDTO.class);
        //维度条件
        List<DimensionConditionBean> dimensionList = reportBuildInDTO.getDimensionCondition();
        //对比条件
        List<CompareConditionBean> compareList = reportBuildInDTO.getCompareCondition();
        //指标条件
        List<IndexConditionBean> indexList = reportBuildInDTO.getIndexCondition();

        SparkSqlCondition condition = new SparkSqlCondition();
        if (!Strings.isNullOrEmpty(reportBuildInDTO.getSql())) {
            condition.setSelectSql(reportBuildInDTO.getSql());
        }
        if (Objects.nonNull(dimensionList)) {
            condition.setGroupList(dimensionList.stream().map(BaseConditionBean::getFieldName).collect(Collectors.toList()));
        }
        if (Objects.nonNull(compareList)) {
            condition.setCompareList(compareList.stream().map(BaseConditionBean::getFieldName).collect(Collectors.toList()));
        }
        if (Objects.nonNull(indexList)) {
            condition.setIndexList(indexList.stream().map(BaseConditionBean::getFieldName).collect(Collectors.toList()));
            condition.setSparkAggMap(aggBuilder(indexList));
        }
        if (Objects.isNull(reportBuildInDTO.getLimit()) || reportBuildInDTO.getLimit() <= 0 || reportBuildInDTO.getLimit() > 1500) {
            condition.setLimit(1500);
        } else {
            condition.setLimit(reportBuildInDTO.getLimit());
        }
        return condition;
    }

    /**
     * 指标聚合类型区分
     *
     * @param indexList 指标对象集合
     * @return java.util.Map<java.lang.String,java.util.List<java.lang.String>>
     * @author 刘凯峰
     * @date 2019/2/25 13:33
     */
    private Map<String, List<String>> aggBuilder( List<IndexConditionBean> indexList ) {
        List<String> sumList = Lists.newArrayList();
        List<String> avgList = Lists.newArrayList();
        List<String> countList = Lists.newArrayList();
        List<String> disCountList = Lists.newArrayList();

        Map<String, List<String>> sparkAggMap = Maps.newLinkedHashMap();
        indexList.forEach(index -> {
            String fieldAliasName = index.getFieldAliasName();
            if (FUNC_SUM.getCode().equals(index.getAggregator())) {
                sumList.add(fieldAliasName);
                sparkAggMap.put(index.getAggregator(), sumList);
            }
            if (FUNC_COUNT.getCode().equals(index.getAggregator())) {
                countList.add(fieldAliasName);
                sparkAggMap.put(index.getAggregator(), countList);
            }
            if (FUNC_AVG.getCode().equals(index.getAggregator())) {
                avgList.add(fieldAliasName);
                sparkAggMap.put(index.getAggregator(), avgList);
            }
            if (FUNC_DISTINCT_COUNT.getCode().equals(index.getAggregator())) {
                disCountList.add(fieldAliasName);
                sparkAggMap.put(index.getAggregator(), disCountList);
            }
        });
        return sparkAggMap;
    }

}

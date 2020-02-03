package org.apache.livy.client.common.ext;


import cn.com.tcsl.cmp.client.dto.report.BiReportBuildInDTO;
import cn.com.tcsl.cmp.client.dto.report.condition.SearchBuilder;
import cn.com.tcsl.cmp.client.dto.report.condition.SparkSqlCondition;
import cn.com.tcsl.cmp.client.dto.report.condition.SqlBuilder;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * to do desc
 *
 * @author kaifeng
 * @date 2019/11/19
 */
public class SparkExtParam {
    public SparkSqlCondition analyzeParam(String param) {
        return JSONObject.parseObject(param, SparkSqlCondition.class);
    }
    public static void main(String[] args) {
        String param="{\"accessToken\":\"bbcb60bb-e6c6-4c57-a7e3-701b0c30d187\",\"compareCondition\":[{\"aliasName\":\"周(营业日)\",\"dataType\":\"datetime\",\"fieldAliasName\":\"ljc_compare_x_settle_biz_dateweek1576140730000_0\",\"fieldDescription\":\"营业日\",\"fieldGroup\":0,\"fieldId\":\"190618195553012179\",\"fieldName\":\"settle_biz_date\",\"granularity\":\"week\",\"isBuildAggregated\":0,\"nanFlag\":0,\"originDataType\":\"datetime\",\"qoqType\":0,\"udfType\":0,\"uniqId\":\"1576140730000\"}],\"computeKind\":\"sql_ext\",\"dataSourceType\":1,\"dbName\":\"e000\",\"dimensionCondition\":[{\"aliasName\":\"POS门店名称\",\"dataType\":\"str\",\"fieldAliasName\":\"ljc_group_x_store_namestr1576140650000_0\",\"fieldDescription\":\"POS门店名称\",\"fieldGroup\":0,\"fieldId\":\"190618195553012342\",\"fieldName\":\"store_name\",\"granularity\":\"str\",\"isBuildAggregated\":0,\"nanFlag\":0,\"originDataType\":\"str\",\"qoqType\":0,\"udfType\":0,\"uniqId\":\"1576140650000\"}],\"filterCondition\":[{\"dataType\":\"double\",\"fieldAliasName\":\"ljc_filter_x_group_codefilter_0\",\"fieldDescription\":\"POS集团编号\",\"fieldGroup\":0,\"fieldName\":\"group_code\",\"fieldValue\":[\"44257\"],\"isBuildAggregated\":0,\"nanFlag\":0,\"originDataType\":\"double\",\"qoqType\":0,\"udfType\":0}],\"indexCondition\":[{\"aggregator\":\"sum\",\"aggregatorName\":\"求和\",\"aliasName\":\"营业额(求和)\",\"dataType\":\"double\",\"fieldAliasName\":\"ljc_sum_x_recv_moneydouble1576140623000_0\",\"fieldDescription\":\"营业额\",\"fieldGroup\":0,\"fieldId\":\"190618195553012273\",\"fieldName\":\"recv_money\",\"granularity\":\"double\",\"isBuildAggregated\":0,\"nanFlag\":0,\"originDataType\":\"double\",\"qoqType\":0,\"udfType\":0,\"uniqId\":\"1576140623000\"},{\"aggregator\":\"sum\",\"aggregatorName\":\"求和-周环比\",\"aliasName\":\"营业额(1)(求和-周环比)\",\"dataType\":\"double\",\"fieldAliasName\":\"ljc_sum_x_recv_moneydouble1576140753000_0\",\"fieldDescription\":\"营业额\",\"fieldGroup\":0,\"fieldId\":\"190618195553012273\",\"fieldName\":\"recv_money\",\"granularity\":\"double\",\"isBuildAggregated\":0,\"nanFlag\":0,\"originDataType\":\"double\",\"qoqConditionBean\":{\"fieldAliasName\":\"ljc_qoq_x_settle_biz_dateweekqoq_0\",\"fieldDescription\":\"营业日\",\"fieldName\":\"settle_biz_date\",\"granularity\":\"week\",\"qoqRadixTime\":\"2019-50\",\"qoqReducedTime\":\"2019-49\",\"qoqResultType\":2},\"qoqType\":2,\"udfType\":0,\"uniqId\":\"1576140753000\"}],\"indexDoubleCondition\":[],\"limit\":-1,\"maxWaitSeconds\":60,\"page\":0,\"platformVersion\":\"0\",\"queryPoint\":0,\"queryType\":0,\"reportCode\":\"191212164849015466\",\"sessionGroup\":\"group_report\",\"sortCondition\":[],\"synSubmit\":true,\"tbId\":\"190618195553000613\",\"tbName\":\"dw_trade_bill_fact_p_group_upgrade\",\"tbType\":\"0\",\"timeoutCancelOpen\":true,\"tracId\":\"15764849790009639294983\"}";
        BiReportBuildInDTO biReportBuildInDTO= JSONObject.parseObject(param, BiReportBuildInDTO.class);
        SqlBuilder sqlBuilder = new SqlBuilder(biReportBuildInDTO);
        SparkSqlCondition sparkSqlCondition= new SearchBuilder().toSparkSql(sqlBuilder);
        System.out.println(JSONObject.toJSONString(sparkSqlCondition));
    }

}

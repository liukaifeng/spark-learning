package org.apache.livy.client.ext;


import cn.com.tcsl.cmp.client.dto.report.condition.SparkSqlCondition;
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

}

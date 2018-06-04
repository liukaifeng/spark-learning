package com.spark;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

/**
 * @package: com.spark
 * @project-name: spark-learning
 * @description: todo 一句话描述该类的用途
 * @author: Created by 刘凯峰
 * @create-datetime: 2018-05-14 17-28
 */
public class TestDemo {
    public static void main( String[] args ) {
        List<String> set = Lists.newArrayList();
        for (int i = 0; i < 5; i++) {
            set.add(String.valueOf(i));
        }
        System.err.println(JSONObject.toJSONString(set));
    }
}

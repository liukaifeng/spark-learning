package com.spark.polling;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @package: com.spark
 * @project-name: spark-learning
 * @description: 轮询（Round Robin）法
 * @author: Created by 刘凯峰
 * @create-datetime: 2018-06-26 13-10
 */
public class RoundRobinPoll {
    private static Integer pos = 0;

    public static String getServer()
    {
        // 重建一个Map，避免服务器的上下线导致的并发问题
        Map<String, Integer> serverMap =
                new HashMap<String, Integer>();
        serverMap.putAll(IpMap.serverWeightMap);

        // 取得Ip地址List
        Set<String> keySet = serverMap.keySet();
        ArrayList<String> keyList = new ArrayList<String>();
        keyList.addAll(keySet);

        String server = null;
        synchronized (pos)
        {
            if (pos > keySet.size()) {
                pos = 0;
            }
            server = keyList.get(pos);
            pos ++;
        }
        return server;
    }
}

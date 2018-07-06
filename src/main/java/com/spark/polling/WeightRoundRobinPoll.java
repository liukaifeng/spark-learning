package com.spark.polling;

import java.util.*;

/**
 * @package: com.spark.polling
 * @project-name: spark-learning
 * @description: 加权轮询
 * @author: Created by 刘凯峰
 * @create-datetime: 2018-06-26 13-14
 */
public class WeightRoundRobinPoll {
    private static Integer pos;

    public static String getServer() {
        // 重建一个Map，避免服务器的上下线导致的并发问题
        Map<String, Integer> serverMap =
                new HashMap<String, Integer>();
        serverMap.putAll(IpMap.serverWeightMap);

        // 取得Ip地址List
        Set<String> keySet = serverMap.keySet();
        Iterator<String> iterator = keySet.iterator();

        List<String> serverList = new ArrayList<String>();
        while (iterator.hasNext()) {
            String server = iterator.next();
            int weight = serverMap.get(server);
            for (int i = 0; i < weight; i++) {
                serverList.add(server);
            }
        }

        String server = null;
        synchronized (pos) {
            if (pos > keySet.size()) {
                pos = 0;
            }
            server = serverList.get(pos);
            pos++;
        }

        return server;
    }
}

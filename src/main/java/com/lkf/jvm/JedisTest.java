package com.lkf.jvm;

import redis.clients.jedis.Jedis;


/**
 * @author 刘凯峰
 * @version V1.0
 * update-logs:方法变更说明
 * ****************************************************
 * name:
 * date:
 * description:
 * *****************************************************
 * @date 2019-12-24 14:59
 * @description TODO
 */
public class JedisTest {
    public static void main(String[] args) {
        //redis服务器地址
        String host = "192.168.5.103";
        //redis 服务端口号
        int port = 6379;
        String filed = "value";
        //Jedis获取到的Redis数据在jedis里
        Jedis jedis = new Jedis(host, port);
        String key = jedis.randomKey();
        String value = jedis.hget(key, filed);
        System.out.println("key:" + key + " value:" + value);
    }
}

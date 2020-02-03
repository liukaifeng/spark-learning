package com.spark;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;

/**
 * todo 一句话描述该类的用途
 *
 * @author 刘凯峰
 * @date 2019-05-20 16-24
 */
public class JedisDemo {
    public static void main( String[] args ) {
//        Set<String> set = Sets.newHashSet();
//        set.add("118.187.7.42:26379");
//        set.add("118.187.7.42:26380");
//        set.add("118.187.7.42:26381");
//        JedisSentinelPool pool = new JedisSentinelPool("redis-master", set, "123456");
//        Jedis jedis = pool.getResource();
//        jedis.set("hello", "world");
//        System.out.println(jedis.get("hello"));

        RedisURI redisUri = RedisURI.Builder.sentinel("118.187.7.42", "redis-master").withSentinel("118.187.7.42").build();
        RedisClient client = RedisClient.create(redisUri);

        StatefulRedisConnection<String, String> conn = client.connect();
    }
}

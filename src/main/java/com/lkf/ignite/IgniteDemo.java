package com.lkf.ignite;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.mxbean.DataStorageMetricsMXBean;

import java.util.List;
import java.util.Map;
import java.util.Random;


/**
 * todo 一句话描述该类的用途
 *
 * @author 刘凯峰
 * @date 2019-06-18 16-55
 */
public class IgniteDemo {
    public static void main( String[] args ) {


        ClientConfiguration cfg = new ClientConfiguration().setAddresses("192.168.12.203:10800","192.168.12.204:10800");

        try (IgniteClient igniteClient = Ignition.startClient(cfg)) {

            System.out.println(">>> Thin client put-get example started.");
            final String CACHE_NAME = "wx_store";
            ClientCache<String, List<Integer>> cache = igniteClient.getOrCreateCache(CACHE_NAME);
            Random random= new Random();
            List<Long> list=Lists.newArrayList();
//            for (int i = 0; i < 200; i++) {
//                long begin= System.currentTimeMillis();
//                String key="门店"+random.nextInt(5000);
//                cache.get(key);
//                long end= System.currentTimeMillis();
//                long hs=end-begin;
//                list.add(hs);
//                System.out.println("第"+i+"次,"+key+"："+hs);
//            }
            System.out.println(JSONObject.toJSONString(list));

        } catch (ClientException e) {
            System.err.println(e.getMessage());
        } catch (Exception e) {
            System.err.format("Unexpected failure: %s\n", e);
        }
    }


}


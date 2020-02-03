package com.lkf.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

import java.util.List;

/**
 * todo 一句话描述该类的用途
 *
 * @author 刘凯峰
 * @date 2019-06-19 15-09
 */
public class IgniteMemoryManager {

    public static void main( String[] args ) {
//        IgniteConfiguration cfg = new IgniteConfiguration();
//        CacheConfiguration cacheCfg = new CacheConfiguration("test-cache");
//        // Enabling the metrics for the cache.
//        cacheCfg.setStatisticsEnabled(true);
//        // Starting the node.
//        Ignite ignite= Ignition.start(cfg);
//        IgniteCache<String, List<Integer>> cache = ignite.getOrCreateCache("wx_store");
//
//        // Get cache metrics
//        CacheMetrics cm = cache.metrics();
//
//        System.out.println("Avg put time: " + cm.getAveragePutTime());
//
//        System.out.println("Avg get time: " + cm.getAverageGetTime());
    }

}

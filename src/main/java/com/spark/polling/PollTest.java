package com.spark.polling;

import com.google.common.collect.Lists;
import org.apache.hadoop.metrics.util.MBeanUtil;

import java.util.List;

/**
 * @package: com.spark.polling
 * @project-name: spark-learning
 * @description: todo 一句话描述该类的用途
 * @author: Created by 刘凯峰
 * @create-datetime: 2018-06-26 16-33
 */
public class PollTest {
    public static void main( String[] args ) {
        String reportCode = "180620145114000010";
        List<String> list = Lists.newArrayList("0", "1", "2");
        int hashCode = reportCode.hashCode();

        //获取线程数
        ThreadGroup threadGroup = Thread.currentThread().getThreadGroup();
        while(threadGroup.getParent() != null){
            threadGroup = threadGroup.getParent();
        }
        int totalThread = threadGroup.activeCount();

        System.out.println(totalThread);
        System.out.println(Math.abs(hashCode) % list.size());

    }
}

package com.lkf.pool;

import org.apache.commons.lang3.RandomUtils;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @package: com.lkf.pool
 * @project-name: spark-learning
 * @description: todo 一句话描述该类的用途
 * @author: Created by 刘凯峰
 * @create-datetime: 2018-06-28 17-56
 */
public class MonitorTest {
    private static volatile boolean stop = false;

    public static void main( String[] args ) throws InterruptedException, IOException {
        // 初始化线程池
        final MonitorableThreadPoolExecutor pool = new MonitorableThreadPoolExecutor(5, 10, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        //添加任务线程监控器，记录任务线程执行时长
        pool.addMonitorTask("TimeMonitorTask", new TimeMonitorHandler());
        // 起一个线程不断地往线程池添加任务
        Thread t = new Thread(() -> {
            startAddTask(pool);
        });
        t.start();

        Thread.sleep(1000);
        stop = true;
        t.join();
        pool.shutdown();
        // 等线程池任务跑完
        pool.awaitTermination(100, TimeUnit.SECONDS);
    }

    // 随机runnable或者callable<?>, 任务随机抛异常
    private static void startAddTask( MonitorableThreadPoolExecutor pool ) {
        int count = 0;
        while (!stop) {
            // Callable<?>任务
            if (RandomUtils.nextBoolean()) {
                pool.submit(() -> {
                    // 随机抛异常
                    boolean bool = RandomUtils.nextBoolean();
                    // 随机耗时 0~100 ms
                    Thread.sleep(RandomUtils.nextInt(0, 100));
                    if (bool) {
                        throw new RuntimeException("thrown randomly");
                    }
                    return bool;
                });
            } else {
                // Runnable任务
                pool.submit(() -> {
                    try {
                        Thread.sleep(RandomUtils.nextInt(1000, 3000));
                    } catch (InterruptedException ignored) {
                    }
                    // 随机抛异常
                    if (RandomUtils.nextBoolean()) {
                        throw new RuntimeException("thrown randomly");
                    }
                });
            }
            System.out.println(String.format("%s:已提交 %d 个任务", time(), ++count));
        }
    }

    private static String time() {
        return String.valueOf(System.currentTimeMillis());
    }
}

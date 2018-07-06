package com.lkf.pool;

import java.util.Map;
import java.util.concurrent.*;


/**
 * 任务监听器，监控任务线程的执行时间
 *
 * @author lkf
 */
public class TimeMonitorHandler implements MonitorHandler {

    private final Map<Runnable, Long> timeRecords = new ConcurrentHashMap<>();

    @Override
    public boolean usable() {
        return true;
    }

    @Override
    public void terminated( int largestPoolSize, long completedTaskCount ) {
        System.out.println(String.format("%s:线程池最大容量=%d, 已经完成的任务数量=%s", time(), largestPoolSize, completedTaskCount));
    }

    @Override
    public void before( Thread thread, Runnable runnable ) {
        System.out.println("-------------------------------------------------------------");
        System.out.println(String.format("%s: before[%s -> %s]", time(), thread, runnable));
        timeRecords.put(runnable, System.currentTimeMillis());
    }

    @Override
    public void after( Runnable runnable, Throwable throwable ) {
        long end = System.currentTimeMillis();
        Long start = timeRecords.remove(runnable);

        Object result = null;
        // 有返回值的异步任务，不一定是Callable<?>，也有可能是Runnable
        if (throwable == null && runnable instanceof FutureTask<?>) {
            try {
                result = ((Future<?>) runnable).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // reset
            } catch (ExecutionException | CancellationException e) {
                throwable = e;
            }
        }
        // 任务正常结束
        if (throwable == null) {
            // 有返回值的异步任务
            if (result != null) {
                System.out.println(String.format("%s: after[当前线程：%s -> 当前任务：%s], 总耗时 %d 毫秒, 执行结果: %s", time(), Thread.currentThread(), runnable, end - start, result));
            } else {
                System.out.println(String.format("%s: after[当前线程：%s -> 当前任务%s], 总耗时 %d 毫秒", time(), Thread.currentThread(), runnable, end - start));
            }
        } else {
            System.err.println(String.format("%s: after[当前线程：%s -> 当前任务%s], 总耗时 %d 毫秒, 异常信息: %s", time(), Thread.currentThread(), runnable, end - start, throwable));
        }
        System.out.println("-------------------------------------------------------------");
    }

    private String time() {
        return String.valueOf(System.currentTimeMillis());
    }
}

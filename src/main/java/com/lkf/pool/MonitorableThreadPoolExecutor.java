package com.lkf.pool;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

/**
 * 可监控的线程池, 可有多个监控处理器,如果监控的逻辑是比较耗时的话, 最好另起线程或者线程池专门用来跑MonitorHandler的方法.
 *
 * @author lkf
 */
public class MonitorableThreadPoolExecutor extends ThreadPoolExecutor {
    /**
     * 可有多个监控处理器
     */
    private final Map<String, MonitorHandler> handlerMap = new HashMap<String, MonitorHandler>();
    /**
     * 虚拟锁对象
     */
    private final Object lock = new Object();

    public MonitorableThreadPoolExecutor( int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, RejectedExecutionHandler handler ) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler);
    }

    public MonitorableThreadPoolExecutor( int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, RejectedExecutionHandler handler ) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
    }

    public MonitorableThreadPoolExecutor( int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory ) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
    }

    public MonitorableThreadPoolExecutor( int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue ) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
    }

    /**
     * 任务执行之前调用该方法
     *
     * @param thread   执行任务的线程
     * @param runnable 将要被执行的任务
     */
    @Override
    protected void beforeExecute( Thread thread, Runnable runnable ) {
        super.beforeExecute(thread, runnable);
        // 依次调用处理器
        for (MonitorHandler handler : handlerMap.values()) {
            if (handler.usable()) {
                handler.before(thread, runnable);
            }
        }
    }

    /**
     * 任务执行之后调用该方法
     *
     * @param runnable  执行完成的任务
     * @param throwable 任务执行过程中如果有异常，抛出的异常信息
     */
    @Override
    protected void afterExecute( Runnable runnable, Throwable throwable ) {
        super.afterExecute(runnable, throwable);
        // 依次调用处理器
        for (MonitorHandler handler : handlerMap.values()) {
            if (handler.usable()) {
                handler.after(runnable, throwable);
            }
        }
    }

    /**
     * 线程被中断时调用该方法
     */
    @Override
    protected void terminated() {
        super.terminated();
        for (MonitorHandler handler : handlerMap.values()) {
            if (handler.usable()) {
                handler.terminated(getLargestPoolSize(), getCompletedTaskCount());
            }
        }

    }

    /**
     * 增加监控任务
     *
     * @param key             监控处理器名称
     * @param task            监控处理器
     * @param overrideIfExist 如果存在是否覆盖
     */
    public MonitorHandler addMonitorTask( String key, MonitorHandler task, boolean overrideIfExist ) {
        if (overrideIfExist) {
            synchronized (lock) {
                return handlerMap.put(key, task);
            }
        } else {
            synchronized (lock) {
                return handlerMap.putIfAbsent(key, task);
            }
        }
    }

    /**
     * 增加监控任务，覆盖同名监控处理器
     *
     * @param key  监控处理器名称
     * @param task 监控处理器
     */
    public MonitorHandler addMonitorTask( String key, MonitorHandler task ) {
        return addMonitorTask(key, task, true);
    }

    /**
     * 移除监控处理器
     *
     * @param key 监控处理器名称
     */
    public MonitorHandler removeMonitorTask( String key ) {
        synchronized (lock) {
            return handlerMap.remove(key);
        }
    }

}

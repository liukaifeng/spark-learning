package com.lkf.jvm;

/**
 * -verbose:gc  表示输出虚拟机中GC的详细情况.
 * -XX:+PrintGCDetails 记录 GC 运行时的详细数据信息，包括新生成对象的占用内存大小以及耗费时间等
 * -XX:+PrintGCDateStamps 打印垃圾收集的时间戳
 * -Xmx20m
 * -Xms20m
 * -Xmn10m
 * -XX:PretenureSizeThreshold=2M 超过该值直接在老年区分配
 * -XX:+UseConcMarkSweepGC 使用cms进行垃圾收集
 * -XX:+UseParNewGC  使用并行收集器
 * -XX:CMSInitiatingOccupancyFraction=60  是指设定CMS在对内存占用率达到60%的时候开始GC
 * -XX:+UseCMSInitiatingOccupancyOnly 只是用设定的回收阈值(上面指定的60%),如果不指定,JVM仅在第一次使用设定值,后续则自动调整.
 *
 * @author 刘凯峰
 * @date 2019-04-16 17-06
 */
public class CmsGcTest {
    private static final int _1M = 1 * 1024 * 1024;
    private static final int _2M = 2 * 1024 * 1024;

    public static void main( String[] args ) {
        ygc(3);
//        cmsGc(1);
//        ygc(2);
//        cmsGc(2);
        // 在这里想怎么触发GC就怎么调用ygc()和cmsGc()两个方法
    }

    /**
     * @param n 预期发生n次young gc
     */
    private static void ygc( int n ) {
        for (int i = 0; i < n; i++) {
            // 由于Eden区设置为8M, 所以分配8个1M就会导致一次YoungGC
            for (int j = 0; j < 8; j++) {
                byte[] tmp = new byte[_1M];
                System.out.println("第 "+(j+1)+" 次");
            }
//            byte[] tmp = new byte[8*_1M];
        }
    }

    /**
     * @param n 预期发生n次CMS gc
     */
    private static void cmsGc( int n ) {
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < 3; j++) {
                // 由于设置了-XX:PretenureSizeThreshold=2M, 所以分配的2M对象不会在Eden区分配而是直接在Old区分配
                byte[] tmp = new byte[_2M];
            }
            try {
                // sleep10秒是为了让CMS GC线程能够有足够的时间检测到Old区达到了触发CMS GC的条件并完成CMS GC, CMS GC线程默认2s扫描一次，可以通过参数CMSWaitDuration配置，例如-XX:CMSWaitDuration=3000
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

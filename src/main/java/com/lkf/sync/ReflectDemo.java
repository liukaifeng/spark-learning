package com.lkf.sync;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @package: com.lkf.sync
 * @project-name: spark-learning
 * @description: todo 一句话描述该类的用途
 * @author: Created by 刘凯峰
 * @create-datetime: 2018-07-02 17-31
 */
public class ReflectDemo {
    static final int COUNT_BITS = Integer.SIZE - 3;
    static final int CAPACITY = (1 << COUNT_BITS) - 1;


    // runState is stored in the high-order bits
    static final int RUNNING = -1 << COUNT_BITS;
    static final int SHUTDOWN = 0 << COUNT_BITS;
    static final int STOP = 1 << COUNT_BITS;
    static final int TIDYING = 2 << COUNT_BITS;
    static final int TERMINATED = 3 << COUNT_BITS;
    static final AtomicInteger ctl = new AtomicInteger(ctlOf(RUNNING, 0));

    public static void main( String[] args ) throws IllegalAccessException, InstantiationException {
//        System.out.println("COUNT_BITS = Integer.SIZE - 3:" + COUNT_BITS);
//        System.out.println("CAPACITY = (1 << COUNT_BITS) - 1:" + CAPACITY);
//        System.out.println("RUNNING = -1 << COUNT_BITS:" + RUNNING + ":" + Integer.toString(RUNNING, 2));
//        System.out.println("SHUTDOWN = 0 << COUNT_BITS:" + SHUTDOWN + ":" + Integer.toString(SHUTDOWN, 2));
//        System.out.println("STOP = 1 << COUNT_BITS:" + STOP + ":" + Integer.toString(STOP, 2));
//        System.out.println("TIDYING = 2 << COUNT_BITS:" + TIDYING + ":" + Integer.toString(TIDYING, 2));
//        System.out.println("TERMINATED = 3 << COUNT_BITS:" + TERMINATED + ":" + Integer.toString(TERMINATED, 2));
//        System.out.println("ctl:" + ctl);
        BlockingQueue<String> blockingQueue = new ArrayBlockingQueue<String>(3);
        blockingQueue.offer("1");
        blockingQueue.offer("2");
        blockingQueue.offer("3");
        blockingQueue.offer("4");

        System.out.println(blockingQueue.size());


//        System.out.println(Integer.toString(1, 2));
//        System.out.println(Integer.toString(2, 2));
//        System.out.println(Integer.toString(3, 2));
//        System.out.println(Integer.toString(4, 2));


// 取模运算       a % (2^n) 等价于 a & (2^n - 1)
// 乘法运算       a * (2^n) 等价于 a << n
// 除法运算       a / (2^n) 等价于 a>> n
// 取余运算       a % 2 等价于 a & 1

//        System.out.println(2 & 3);
    }

    private static int runStateOf( int c ) {
        return c & ~CAPACITY;
    }

    private static int workerCountOf( int c ) {
        return c & CAPACITY;
    }

    private static int ctlOf( int rs, int wc ) {
        return rs | wc;
    }
}

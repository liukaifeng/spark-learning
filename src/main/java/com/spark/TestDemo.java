package com.spark;

import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

/**
 * @package: com.spark
 * @project-name: spark-learning
 * @description: todo 一句话描述该类的用途
 * @author: Created by 刘凯峰
 * @create-datetime: 2018-05-14 17-28
 */
public class TestDemo {
    private static final int COUNT_BITS = Integer.SIZE - 3;
    private static final int CAPACITY = (1 << COUNT_BITS) - 1;

    // runState is stored in the high-order bits
    private static final int RUNNING = -1 << COUNT_BITS;
    private static final int SHUTDOWN = 0 << COUNT_BITS;
    private static final int STOP = 1 << COUNT_BITS;
    private static final int TIDYING = 2 << COUNT_BITS;
    private static final int TERMINATED = 3 << COUNT_BITS;

    // Packing and unpacking ctl
    private static int runStateOf( int c ) {
        return c & ~CAPACITY;
    }

    private static int workerCountOf( int c ) {
        return c & CAPACITY;
    }

    private static int ctlOf( int rs, int wc ) {
        return rs | wc;
    }

    public static void main( String[] args ) {
        List<String> list = new LinkedList<>();
        for (int i = 0; i < 10; i++) {
            list.add("E-" + i);
        }
        ListIterator<String> it = list.listIterator(list.size());
        while (it.hasPrevious()) {
            System.out.println(it.previous());
        }

    }
}

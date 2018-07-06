package com.lkf.sync;

/**
 * @package: com.lkf.sync
 * @project-name: spark-learning
 * @description: 两个线程访问同一个对象中的同步方法一定是线程安全的。
 * 本实现由于是同步访问，所以先执行完一个线程再执行另外一个
 * @author: Created by 刘凯峰
 * @create-datetime: 2018-06-29 20-19
 */
public class UnSafeThread {
    public static void main( String[] args ) {
//1、多个线程访问同一个对象的同步方法
//        HasSelfPrivateNum numRef = new HasSelfPrivateNum();
//
//        ThreadA athread = new ThreadA(numRef);
//        athread.start();
//
//        ThreadB bthread = new ThreadB(numRef);
//        bthread.start();


       /* 2、多个对象多个锁
          这里是非同步的，因为线程athread获得是numRef1的对象锁，
         而bthread线程获取的是numRef2的对象锁，他们并没有在获取锁上有竞争关系，因此，出现非同步的结果*/
        HasSelfPrivateNum numRef1 = new HasSelfPrivateNum();
        HasSelfPrivateNum numRef2 = new HasSelfPrivateNum();

        ThreadA athread = new ThreadA(numRef1);
        athread.start();

        ThreadB bthread = new ThreadB(numRef2);
        bthread.start();

    }
}

class HasSelfPrivateNum {

    private int num = 0;

    synchronized public void addI( String username ) {
        try {
            if (username.equals("a")) {
                num = 100;
                System.out.println("a set over!");
                Thread.sleep(2000);
            } else {
                num = 200;
                System.out.println("b set over!");
            }
            System.out.println(username + " num=" + num);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}

class ThreadA extends Thread {

    private HasSelfPrivateNum numRef;

    public ThreadA( HasSelfPrivateNum numRef ) {
        super();
        this.numRef = numRef;
    }

    @Override
    public void run() {
        super.run();
        numRef.addI("a");
    }

}

class ThreadB extends Thread {

    private HasSelfPrivateNum numRef;

    public ThreadB( HasSelfPrivateNum numRef ) {
        super();
        this.numRef = numRef;
    }

    @Override
    public void run() {
        super.run();
        numRef.addI("b");
    }

}

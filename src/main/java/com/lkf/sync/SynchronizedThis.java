package com.lkf.sync;

/**
 * @package: com.lkf.sync
 * @project-name: spark-learning
 * @description: 线程获取的是同步块synchronized (this)括号里面的对象实例的对象锁，
 * 这里就是ObjectService实例对象的对象锁了。
 * @author: Created by 刘凯峰
 * @create-datetime: 2018-06-29 20-30
 */
public class SynchronizedThis {
    public static void main( String[] args ) {
        ObjectService service = new ObjectService();

        SynchronizedThisThreadA a = new SynchronizedThisThreadA(service);
        a.setName("a");
        a.start();

        SynchronizedThisThreadB b = new SynchronizedThisThreadB(service);
        b.setName("b");
        b.start();
    }
}

class ObjectService {
    public void serviceMethod() {
        try {
            synchronized (this) {
                System.out.println("begin time=" + System.currentTimeMillis());
                Thread.sleep(2000);
                System.out.println("end    end=" + System.currentTimeMillis());
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

class SynchronizedThisThreadA extends Thread {

    private ObjectService service;

    public SynchronizedThisThreadA( ObjectService service ) {
        super();
        this.service = service;
    }

    @Override
    public void run() {
        super.run();
        service.serviceMethod();
    }

}

class SynchronizedThisThreadB extends Thread {
    private ObjectService service;

    public SynchronizedThisThreadB( ObjectService service ) {
        super();
        this.service = service;
    }

    @Override
    public void run() {
        super.run();
        service.serviceMethod();
    }
}


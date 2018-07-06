package com.lkf.sync;

/**
 * @package: com.lkf.sync
 * @project-name: spark-learning
 * @description: 这里线程争夺的是anyString的对象锁，两个线程有竞争同一对象锁的关系，出现同步
 * <p>
 * 现在有一个问题：一个类里面有两个非静态同步方法，会有影响么？
 * <p>
 * 答案是：如果对象实例A，线程1获得了对象A的对象锁，那么其他线程就不能进入需要获得对象实例A的对象锁才能访问的同步代码（包括同步方法和同步块）。不理解可以细细品味一下！
 * @author: Created by 刘凯峰
 * @create-datetime: 2018-06-29 20-48
 */
public class NonSynchronizedThis {
    public static void main( String[] args ) {
        Service service = new Service("测试");

        NonSynchronizedThisThreadA a = new NonSynchronizedThisThreadA(service);
        a.setName("A");
        a.start();

        NonSynchronizedThisThreadB b = new NonSynchronizedThisThreadB(service);
        b.setName("B");
        b.start();
    }
}

class Service {
    String anyString = new String();

    public Service( String anyString ) {
        this.anyString = anyString;
    }

    public void setUsernamePassword( String username, String password ) {
        try {
            synchronized (anyString) {
                System.out.println("线程名称为：" + Thread.currentThread().getName()
                        + "在" + System.currentTimeMillis() + "进入同步块");
                Thread.sleep(3000);
                System.out.println("线程名称为：" + Thread.currentThread().getName()
                        + "在" + System.currentTimeMillis() + "离开同步块");
            }
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}

class NonSynchronizedThisThreadA extends Thread {
    private Service service;

    public NonSynchronizedThisThreadA( Service service ) {
        super();
        this.service = service;
    }

    @Override
    public void run() {
        service.setUsernamePassword("a", "aa");

    }
}

class NonSynchronizedThisThreadB extends Thread {
    private Service service;

    public NonSynchronizedThisThreadB( Service service ) {
        super();
        this.service = service;
    }

    @Override
    public void run() {
        service.setUsernamePassword("b", "bb");
    }
}

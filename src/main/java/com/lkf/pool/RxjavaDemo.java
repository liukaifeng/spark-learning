package com.lkf.pool;

import io.reactivex.Observable;

/**
 * todo 一句话描述该类的用途
 *
 * @author 刘凯峰
 * @date 2018-11-07 09-00
 */
public class RxjavaDemo {
    public static void main( String[] args ) {
//        Observable<String> observable = Observable.just("Event")
//                .publish()
//                .autoConnect(2)
//                .map(s -> {
//                    System.out.println("Expensive operation for " + s);
//                    return s;
//                });
//
//        observable.subscribe(s -> System.out.println("Sub1 got: " + s));
//        observable.subscribe(s -> System.out.println("Sub2 got: " + s));


        Observable<String> observable = Observable.just("Event")
                .map(s -> {
                    System.out.println("Expensive operation for " + s);
                    return s;
                })
                .publish()
                .autoConnect(2);

        observable.subscribe(s -> System.out.println("Sub1 got: " + s));
        observable.subscribe(s -> System.out.println("Sub2 got: " + s));
    }
}

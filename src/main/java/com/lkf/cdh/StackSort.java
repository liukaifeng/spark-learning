package com.lkf.cdh;

import java.util.Stack;

/**
 * todo 一句话描述该类的用途
 *  将要排序的栈记为stack，申请的辅助栈记为help。在stack上执行pop操作，弹出的元素记为cur。
 * <p>
 *     （1）如果cur小于或等于help的栈顶元素，则将cur直接压入help；
 * <p>
 *      （2）如果cur大于help的栈顶元素，则将该help的元素逐个弹出，逐个压入stack栈，直到cur小于等于help的栈顶元素，再将 cur压入help。
 * <p>
 * <p>
 * 使用了两个栈和一个变量，把排序栈中的第一个元素压入辅助栈中，
 * 如果排序栈中新弹出的cur元素比辅助栈的元素大的话，就把辅助栈中的元素压回排序栈，把cur元素压入辅助栈中。不断重复此过程，辅助栈中的元素始终是从小到大排序。
 *
 * @author 刘凯峰
 * @date 2019-03-19 10-53
 */
public class StackSort {
    public static void main( String[] args ) {
        Stack<Integer> s = new Stack<>();
        s.push(3);
        s.push(5);
        s.push(9);
        s.push(6);
        s.push(2);
        System.out.println(sortStackByStack(s));
    }

    private static Stack<Integer> sortStackByStack( Stack<Integer> stack ) {
        //辅助栈
        Stack<Integer> help = new Stack<Integer>();
        while (!stack.isEmpty()) {
            //排序栈的栈顶元素
            int cur = stack.pop();
            //如果辅助栈不为空并且排序栈的栈顶元素大于辅助栈
            while (!help.isEmpty() && cur > help.peek()) {
                stack.push(help.pop());
            }
            //如果cur小于或等于help的栈顶元素，则将cur直接压入help
            help.push(cur);
        }
        while (!help.isEmpty()) {
            stack.push(help.pop());
        }
        return stack;
    }


}

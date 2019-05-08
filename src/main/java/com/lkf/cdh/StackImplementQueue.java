package com.lkf.cdh;

import java.util.Stack;

/**
 * todo 一句话描述该类的用途
 *
 * @author 刘凯峰
 * @date 2019-03-20 10-22
 */
public class StackImplementQueue {
    public static void main( String args[] ) {
        StackForQueue myQueue = new StackForQueue();
        myQueue.insert(1);
        System.out.println(myQueue.peekBack()); //Will print 1
        // instack: [(top) 1]
        // outStack: []
        myQueue.insert(2);
        System.out.println(myQueue.peekBack()); //Will print 2
        // instack: [(top) 2, 1]
        // outStack: []
        myQueue.insert(3);
        System.out.println(myQueue.peekBack()); //Will print 3
        // instack: [(top) 3, 2, 1]
        // outStack: []
        myQueue.insert(4);
        System.out.println(myQueue.peekBack()); //Will print 4
        // instack: [(top) 4, 3, 2, 1]
        // outStack: []

        System.out.println(myQueue.isEmpty()); //Will print false

        System.out.println(myQueue.remove()); //Will print 1
        System.out.println(myQueue.peekBack()); //Will print NULL
        // instack: []
        // outStack: [(top) 2, 3, 4]

        myQueue.insert(5);
        System.out.println(myQueue.peekFront()); //Will print 2
        // instack: [(top) 5]
        // outStack: [(top) 2, 3, 4]

        myQueue.remove();
        System.out.println(myQueue.peekFront()); //Will print 3
        // instack: [(top) 5]
        // outStack: [(top) 3, 4]
        myQueue.remove();
        System.out.println(myQueue.peekFront()); //Will print 4
        // instack: [(top) 5]
        // outStack: [(top) 4]
        myQueue.remove();
        // instack: [(top) 5]
        // outStack: []
        System.out.println(myQueue.peekFront()); //Will print 5
        // instack: []
        // outStack: [(top) 5]
        myQueue.remove();
        // instack: []
        // outStack: []

        System.out.println(myQueue.isEmpty()); //Will print true

    }

}


class StackForQueue {
    /**
     * 存储入队元素
     */
    private Stack inStack;
    /**
     * 出队时，将 inStack 所有元素放入 outStack 并从 outStack 出队
     */
    private Stack outStack;

    /**
     * 构造函数
     */
    public StackForQueue() {
        this.inStack = new Stack();
        this.outStack = new Stack();
    }

    /**
     * 插入元素到队列
     *
     * @param x 插入的元素
     */
    public void insert( Object x ) {
        // Insert element into inStack
        this.inStack.push(x);
    }

    /**
     * 删除队列，队头的元素
     */
    public Object remove() {
        if (this.outStack.isEmpty()) {
            // 将入栈元素全部放入出栈中
            while (!this.inStack.isEmpty()) {
                this.outStack.push(this.inStack.pop());
            }
        }
        return this.outStack.pop();
    }

    /**
     * 获取队列中队头的元素
     *
     * @return 队头
     */
    public Object peekFront() {
        if (this.outStack.isEmpty()) {
            // Move all elements from inStack to outStack (preserving the order)
            while (!this.inStack.isEmpty()) {
                this.outStack.push(this.inStack.pop());
            }
        }
        return this.outStack.peek();
    }

    /**
     * 获取队列中队尾的元素
     *
     * @return 队尾元素
     */
    public Object peekBack() {
        if (inStack.isEmpty()) {
            return null;
        }
        return this.inStack.peek();
    }

    /**
     * 判断队列是否为空
     *
     * @return 如果队列为空，返回true
     */
    public boolean isEmpty() {
        return (this.inStack.isEmpty() && this.outStack.isEmpty());
    }

}

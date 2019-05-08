package com.lkf.cdh;

/**
 * 单链表节点
 *
 * @author 刘凯峰
 * @date 2019-03-06 17-28
 */
public class SingleLinkedListNode {
    //数据域
    private int data;
    //指针域
    private SingleLinkedListNode next;


    public SingleLinkedListNode( int data ) {
        this.data = data;
    }

    public int getData() {
        return data;
    }

    public void setData( int data ) {
        this.data = data;
    }

    public SingleLinkedListNode getNext() {
        return next;
    }

    public void setNext( SingleLinkedListNode next ) {
        this.next = next;
    }
}

package com.lkf.cdh;

import com.alibaba.fastjson.JSONObject;

/**
 * todo 一句话描述该类的用途
 *
 * @author 刘凯峰
 * @date 2019-03-06 16-32
 */
public class ReversalLinkedList {

   


    /**
     * 递归实现单链表反转
     * <p>
     * 递归实质上就是系统帮你压栈的过程，系统在压栈的时候会保留现场。
     * </p>
     * 我们来看是怎样的一个递归过程：1->2->3->4
     * <p>
     * 程序到达Node newHead = reverse(head.next);时进入递归
     * 我们假设此时递归到了3结点，此时head=3结点，此时head.next(实际上是4结点)
     * 执行Node newHead = reverse(head.next);传入的head.next是4结点，返回的newHead是4结点。
     * <p>
     * 接下来就是弹栈过程了
     * 程序继续执行 temp.next = head就相当于4->3
     * head.next = null 即把3结点指向4结点的指针断掉。
     * 返回新链表的头结点newHead
     *
     * @param head 节点
     * @return com.lkf.cdh.ReversalLinkList.SingleLinkedListNode
     * @author 刘凯峰
     * @date 2019/3/6 17:02
     */
    public static SingleLinkedListNode recursionReverse( SingleLinkedListNode head ) {
        System.out.println("head："+JSONObject.toJSONString(head));

        //如果是空链表或者尾结点
        if (head == null || head.getNext() == null) {
            return head;
        }

        //先反转后续结点
        SingleLinkedListNode newNode = recursionReverse(head.getNext());
        System.out.println("newNode："+JSONObject.toJSONString(newNode));

        //当前结点指针指向前一结点
        head.getNext().setNext(head);

        //令前一结点的指针域为null
        head.setNext(null);
        return newNode;
    }

    /**
     * 遍历实现单链表反转
     *
     * @param head 节点
     * @return com.lkf.cdh.ReversalLinkList.SingleLinkedListNode
     * @author 刘凯峰
     * @date 2019/3/6 17:03
     */
    public static SingleLinkedListNode foreachReverse( SingleLinkedListNode head ) {
        //前一个节点指针
        SingleLinkedListNode preNode = null;
        //当前节点指针
        SingleLinkedListNode curNode = head;
        //下一个节点指针
        SingleLinkedListNode nextNode = null;

        while (curNode != null) {
            //nextNode 指向下一个节点
            nextNode = curNode.getNext();
            //将当前节点next域指向前一个节点
            curNode.setNext(preNode);
            //preNode 指针向后移动
            preNode = curNode;
            //curNode指针向后移动
            curNode = nextNode;
        }
        return preNode;
    }


    public static void main( String[] args ) {
        SingleLinkedListNode node1 = new SingleLinkedListNode(1);
        SingleLinkedListNode node2 = new SingleLinkedListNode(2);
        SingleLinkedListNode node3 = new SingleLinkedListNode(3);
        SingleLinkedListNode node4 = new SingleLinkedListNode(4);
        node1.setNext(node2);
        node2.setNext(node3);
        node3.setNext(node4);
        node4.setNext(null);

//        System.out.println(JSONObject.toJSONString(node1));

//        System.out.println(JSONObject.toJSONString(foreachReverse(node1)));
        System.out.println(JSONObject.toJSONString(recursionReverse(node1)));

    }


}

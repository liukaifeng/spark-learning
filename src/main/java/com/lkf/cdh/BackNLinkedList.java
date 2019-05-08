package com.lkf.cdh;

/**
 * todo 一句话描述该类的用途
 *
 * @author 刘凯峰
 * @date 2019-03-06 17-59
 */
public class BackNLinkedList {
    public SingleLinkedListNode findElem(SingleLinkedListNode head,int k){
        if(k<1 || head == null)
        {
            return null;
        }
        SingleLinkedListNode p1 = head;
        SingleLinkedListNode p2 = head;
        for (int i = 0; i < k - 1; i++) {
            if(p1.getNext() != null){
                p1 = p1.getNext();
            }else {
                return null;
            }

        }
        while (p1 != null) {
            p1 = p1.getNext();
            p2 = p2.getNext();
        }
        return p2;
    }

}

package com.lkf.cdh;

import java.util.Arrays;

/**
 * 1 3 5 7
 * 2 4 6 8
 * <p>
 * 1 3 5 7 0 0 0 0
 * <p>
 * <p>
 * <p>
 * 7 比 8 》 1 3 5 7 0 0 0 8
 * 7 比 6 》 1 3 5 7 0 0 7 8
 * 5 比 6 》 1 3 5 7 0 6 7 8
 * 5 比 4 》 1 3 5 7 5 6 7 8
 * 3 比 4 》 1 3 5 4 5 6 7 8
 * 3 比 2 》 1 3 3 4 5 6 7 8
 * 1 比 2 》 1 2 3 4 5 6 7 8
 *
 * @author 刘凯峰
 * @date 2019-03-18 14-36
 */
public class ArrayMergeSorted {
    public static void main( String[] args ) {
        int[] a = {1, 3, 5, 7, 0, 0, 0, 0};
        int[] b = {2, 4, 6, 8};
        merge(a, 4, b, b.length);
        System.out.println(Arrays.toString(a));
    }


    /**
     * 合并两个有序数组
     *
     * @param a    数组a
     * @param aLng 数组a长度
     * @param b    数组b
     * @param bLng 数组b长度
     * @return void
     * @author 刘凯峰
     * @date 2019/3/18 17:42
     */
    public static void merge( int[] a, int aLng, int[] b, int bLng ) {
        int totalLength = aLng + bLng - 1;
        --aLng;
        --bLng;
        while (bLng >= 0) {
            System.out.println("aLength：" + aLng + "  a[aLength]：" + a[aLng] + "  bLength：" + bLng + "  b[bLength]：" + b[bLng]);
            a[totalLength--] = (aLng >= 0 && a[aLng] > b[bLng]) ? a[aLng--] : b[bLng--];
        }
    }

}

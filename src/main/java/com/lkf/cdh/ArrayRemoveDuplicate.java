package com.lkf.cdh;

import java.util.Arrays;

/**
 * 删除有序数组中的重复元素
 * <p>
 * * 可以放置两个指针 i 和 j，其中 i 是慢指针，而 j 是快指针。只要
 * nums[i]=nums[j]，我们就增加 j 以跳过重复项
 * 当我们遇到 nums[j]≠nums[i] 时，跳过重复项的运行已经结束，因此我们必须把它（nums[j]）的值复
 * 制到 nums[i+1]。然后递增 i，接着我们将再次重复相同的过程，直到 j 到达数组的末尾为止。
 *
 * @author 刘凯峰
 * @date 2019-03-18 15-52
 */
public class ArrayRemoveDuplicate {
    /**
     * @param nums 有序数组
     * @return
     */
    public static int removeDuplicate( int[] nums ) {
        if (nums.length == 0) {
            return 0;
        }
        //i 指向第一个元素
        int i = 0;
        //从第二个元素开始遍历
        for (int j = 1; j < nums.length; j++) {
            //如果 nums[j] != nums[i] 说明两个元素不重复，将 nums[j] 值赋给 nums[i]，同时，i 的值递增
            if (nums[j] != nums[i]) {
                i++;
                nums[i] = nums[j];
            }
            System.out.println("i=" + i + " j=" + j + " nums[i]=" + nums[i] + " nums[j]=" + nums[j] + "  nums=" + Arrays.toString(nums));
        }
        return i + 1;
    }

    public static void main( String[] args ) {
        int[] nums = {0, 0, 1, 1, 1, 2, 2, 3, 3};
        System.out.println("新数组长度："+removeDuplicate(nums));
    }
}

package com.ylqdh.java.learn;

import java.util.Arrays;

// 冒泡排序
public class BubbleSort {
    public static int[] bbsort(int[] source) {
        int[] arr = source;
        int temp;
        boolean flag = true;  // 设置一个标记
        int sum = 0;   // 元素交换的次数
        for (int i = 1; i < arr.length; i++) {
            // 如果j的条件是j<arr.length会更慢，需要比较更多次
            for (int j=0;j<arr.length-i;j++) {
                if (arr[j+1]<arr[j]){
                    temp = arr[j];
                    arr[j] = arr[j+1];
                    arr[j+1] = temp;

                    flag = false;   // 哪怕有一个元素进行了交换，就把标记变为false
                    sum++;
                }
            }

            // 如果一轮循环下来没有一个元素进行了交换，说明了这些元素已经是有序的了，那么可以结束剩下的比较以节省时间
            if (flag) {
                break;
            }
        }
        System.out.println("exchange times: " + sum);
        return arr;
    }

    public static void main(String[] args) {
        int[] sour = {15,1,4,2,5,12,9,3};
        System.out.println(Arrays.toString(bbsort(sour)));
    }
}
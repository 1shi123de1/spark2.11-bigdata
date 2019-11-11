package com.ylqdh.java.learn;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Vector;

public class arrayListTest {
    public static void main(String[] args) {
        ArrayList list = new ArrayList();
        list.add("234");
        list.add("456");
        list.add("345");

        Vector vector = new Vector();
        vector.add(1);

        LinkedList linkedList = new LinkedList();

        Collections.sort(list);
        System.out.println(list);
    }
}

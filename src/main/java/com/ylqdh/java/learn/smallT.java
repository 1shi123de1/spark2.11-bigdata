package com.ylqdh.java.learn;

public class smallT {
    public static void main(String[] args) {
        smallT t = new smallT();
        int b = t.get();
        System.out.println("b= " + b);
    }

    public int get(){
        try{
            System.out.println("try statement");
            return 1;
        }finally {
            System.out.println("finally statement");
            return 2;
        }
    }
}

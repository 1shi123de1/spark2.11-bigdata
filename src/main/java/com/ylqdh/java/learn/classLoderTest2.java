package com.ylqdh.java.learn;

public class classLoderTest2 {
    public static void main(String[] args) {
        Singleton singleton = Singleton.getSingleton();
        System.out.println("counter1=" + Singleton.counter1);
        System.out.println("counter1=" + Singleton.counter2);
    }
}


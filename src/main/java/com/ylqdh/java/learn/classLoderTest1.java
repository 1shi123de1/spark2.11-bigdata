package com.ylqdh.java.learn;

public class classLoderTest1 {
    public static void main(String[] args) {
        System.out.println("Bootstrap ClassLoader path: ");
        System.out.println(System.getProperty("sun.boot.class.path"));
        System.out.println("----------------------------");

        System.out.println("Extension ClassLoader path: ");
        System.out.println(System.getProperty("java.ext.dirs"));
        System.out.println("----------------------------");

        System.out.println("App ClassLoader path: ");
        System.out.println(System.getProperty("java.class.path"));
        System.out.println("----------------------------");
    }
}

class FinalTest {
    public static final int x = 6 / 3;
    static {
        System.out.println("final test");

    }
}

package com.ylqdh.java.learn;

import java.util.concurrent.TimeUnit;

public class synchTest2 {
    // syncObject1,syncObject2 是特殊的对象实例,用长度为0的byte数组是因为占用的内存最少
    private byte[] syncObject1 = new byte[0];
    private byte[] syncObject2 = new byte[0];
    public void f() {
        // other operations should not be locked...
        System.out.println(Thread.currentThread().getName()
                + ":not synchronized in f()");
        synchronized (this) {
            for (int i = 0; i < 5; i++) {
                System.out.println(Thread.currentThread().getName()
                        + ":synchronized in f() --> " + i);
                try {
                    TimeUnit.SECONDS.sleep(3);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void g() {
        // other operations should not be locked...
        System.out.println(Thread.currentThread().getName()
                + ":not synchronized in g()");
        synchronized (syncObject1) {
            for (int i = 0; i < 5; i++) {
                System.out.println(Thread.currentThread().getName()
                        + ":synchronized in g() --> " + i);
                try {
                    TimeUnit.SECONDS.sleep(3);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void h() {
        // other operations should not be locked...
        System.out.println(Thread.currentThread().getName()
                + ":not synchronized in h()");
        synchronized (syncObject2) {
            for (int i = 0; i < 5; i++) {
                System.out.println(Thread.currentThread().getName()
                        + ":synchronized in h() --> " + i);
                try {
                    TimeUnit.SECONDS.sleep(3);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    // 会启动3个线程，main函数一个，两个thread各一个
    // main线程先启动，从上往下执行代码，到两个thread各启动一个线程
    // 启动三个线程，各自都进入准备状态，因为synchronized锁在不同的实例对象(对象分别是this,syncObject1,syncObject2);所以会三个线程同时往下执行，直到方法结束
    public static void main(String[] args) {
        final synchTest2 rs = new synchTest2();

        new Thread() {
            public void run() {
                rs.f();
            }
        }.start();

        new Thread() {
            public void run() {
                rs.g();
            }
        }.start();

        rs.h();
    }
}

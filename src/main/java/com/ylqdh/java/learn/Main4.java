package com.ylqdh.java.learn;

import java.util.concurrent.ExecutionException;

public class Main4 extends Thread {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Main4 m1 = new Main4();
        Main4 m2 = new Main4();
        m1.start();
//        m1.join();
        System.out.println("---------------main---------------");
        m2.start();
        System.out.println("------------main end---------------");
    }

    @Override
    public void run() {
        int i = 0;
        for (i = 0; i < 10; i++) {
            System.out.println(Thread.currentThread().getName() + "[i="+i+"]");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
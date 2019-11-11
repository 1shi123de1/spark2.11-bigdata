package com.ylqdh.java.learn;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class lockTest1 {
    private Lock lock = new ReentrantLock();

    public void f(){
        // other operations should not be locked...
        System.out.println(Thread.currentThread().getName()
                + ":not synchronized in f()");
        lock.lock();
        try {
            for (int i = 0; i < 5; i++) {
                System.out.println(Thread.currentThread().getName()
                        + ":synchronized in f() --> " + i);
                try {
                    TimeUnit.SECONDS.sleep(2);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public void g(){
        // other operations should not be locked...
        System.out.println(Thread.currentThread().getName()
                + ":not synchronized in g()");
        lock.lock();
        try {
            for (int i = 0; i < 5; i++) {
                System.out.println(Thread.currentThread().getName()
                        + ":synchronized in g() --> " + i);
                try {
                    TimeUnit.SECONDS.sleep(2);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public void h(){
        // other operations should not be locked...
        System.out.println(Thread.currentThread().getName()
                + ":not synchronized in h()");
        lock.lock();
        try {
            for (int i = 0; i < 5; i++) {
                System.out.println(Thread.currentThread().getName()
                        + ":synchronized in h() --> " + i);
                try {
                    TimeUnit.SECONDS.sleep(2);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    // ReentrantLock 是Lock的接口实现类,一定要在finally块中释放锁
    // 由于三个方法获取的是同一个lock，所以会出现竞争cpu资源，谁先获得lock锁，就可以先执行方法的同步代码，其他的方法会阻塞
    // 一个方法执行完，剩下的方法才继续竞争cpu资源，一直到全部线程结束
    // 实现的效果和synchTest1一样
    public static void main(String[] args) {
        final lockTest1 rs = new lockTest1();

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

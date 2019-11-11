package com.ylqdh.java.learn;

import java.util.concurrent.*;

public class ThreadPoolExecutorTest {
    public static void main(String[] args) {

        // 正常的线程池创建方式
        int poolSize = Runtime.getRuntime().availableProcessors() * 2;
        BlockingQueue<Runnable> queue = new ArrayBlockingQueue<Runnable>(512);;
        RejectedExecutionHandler policy = new ThreadPoolExecutor.DiscardPolicy();
        ExecutorService executorService = new ThreadPoolExecutor(poolSize,poolSize,0,TimeUnit.SECONDS,queue,policy);

    }
}

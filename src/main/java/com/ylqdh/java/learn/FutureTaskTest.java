package com.ylqdh.java.learn;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

public class FutureTaskTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Callable<Integer> call = new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                System.out.println("callable thread is running");
                return new Random().nextInt();
            }
        };

        FutureTask<Integer> task = new FutureTask<Integer>(call);

        Thread thread = new Thread(task);
        thread.start();

        System.out.println("main thread is doing....");

        Integer result = task.get();

        System.out.println("call result is : " + result);
    }
}

package com.longmaosoft;

import java.util.Date;

import static java.util.concurrent.TimeUnit.SECONDS;

public class DBSync {
    public static void main(String[] args) {
        int i = 0;
        for (; ; ) {
            System.out.println(new Date() + ",DBSync====" + i++);
            try {
                SECONDS.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}


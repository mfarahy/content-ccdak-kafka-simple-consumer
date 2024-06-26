package com.linuxacademy.ccdak.kafkaSimpleConsumer;

import java.util.concurrent.CountDownLatch;

public class Main {

    public static void main(String[] args) {

        System.out.println("Hi Latch!");

            final CountDownLatch latch = new CountDownLatch(1);
    }
}

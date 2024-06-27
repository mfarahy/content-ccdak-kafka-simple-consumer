package com.linuxacademy.ccdak.kafkaSimpleConsumer;

public class Main {

    public static void main(String[] args) {
        if (args[0].equals("consumer")) {
            PurchaseConsumer consumer = new PurchaseConsumer();
            consumer.consume(Integer.parseInt(args[1]), args[2]);
        } else {
            PurchaseProducer producer = new PurchaseProducer();
            producer.produce();
        }
    }
}

package com.linuxacademy.ccdak.kafkaSimpleConsumer;

import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.Random;

public class Main {

    public static void main(String[] args) {

        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class.getName());
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.RETRIES_CONFIG, "1");
        config.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        KafkaProducer<String, Purchase> producer = new KafkaProducer<>(config);

        String[] products = new String[]{"Apricot", "Banana", "Blackberry", "Blueberry", "Canary Melon"};
        Random rnd = new Random(System.currentTimeMillis());
        int i = 0;
        while (true) {
            Purchase purchase = new Purchase((i + 1) * 11, products[rnd.nextInt(products.length)], rnd.nextInt(20) + 1);
            ProducerRecord<String, Purchase> record = new ProducerRecord<>("inventory_purchases", String.valueOf(purchase.getId()), purchase);

            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    System.out.print(".");
                }
            });

            try {
                Thread.sleep(rnd.nextInt(1000) + 200);
            } catch (InterruptedException e) {
                System.out.println(e.getMessage());
                System.exit(-1);
            }
        }
    }
}

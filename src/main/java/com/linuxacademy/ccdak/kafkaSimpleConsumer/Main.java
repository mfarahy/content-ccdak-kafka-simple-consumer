package com.linuxacademy.ccdak.kafkaSimpleConsumer;

import org.apache.kafka.clients.producers.*;
import java.util.Properties;

public class Main {

    public static void main(String[] args) {

        Properties config = new Properties();
        config.put("ProducerConfig", "localhost:9092");
        config.put("key.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class.getName());
        config.put("value.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class.getName());
        config.put("client.id", "schema-registry-lab");
        config.put("allow.auto.create.topics", "false");

        KafkaProducer<String, Purchase> consumer = new KafkaProducer<>(config);
    }
}

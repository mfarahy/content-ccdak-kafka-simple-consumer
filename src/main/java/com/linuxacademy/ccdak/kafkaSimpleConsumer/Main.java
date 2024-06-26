package com.linuxacademy.ccdak.kafkaSimpleConsumer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class Main {

    public static void main(String[] args) {

        if (args.length == 0) {
            System.out.println("No file is provided!");
            System.exit(-1);
            return;
        }

        File file = new File(args[0]);
        if (!file.exists()) {
            System.out.println("File not found!");
            System.exit(-1);
            return;
        }

        Properties config = new Properties();
        config.put("bootstrap.servers", "localhost:9092");
        config.put("key.serializer", org.apache.kafka.common.serialization.StringSerializer.class.getName());
        config.put("value.serializer", org.apache.kafka.common.serialization.StringSerializer.class.getName());
        config.put("client.id", "file-to-topic");
        config.put("acks", "1");

        try (Producer<String, String> producer = new KafkaProducer<String, String>(config)) {
            try {
                try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                    reader.lines().forEach(line -> {
                        if (line != null && !line.trim().isEmpty()) {
                            String[] frags = line.split(":");
                            if (frags.length != 2) return;
                            if (frags[0].equals("apples")) {
                                ProducerRecord<String, String> record = new ProducerRecord<String, String>("apple_purchases", frags[0], frags[1]);
                                producer.send(record);
                            }

                            ProducerRecord<String, String> record = new ProducerRecord<String, String>("inventory_purchases", frags[0], frags[1]);
                            producer.send(record);
                        }
                    });
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}

package com.linuxacademy.ccdak.kafkaSimpleConsumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class Main {

    public static void main(String[] args) {

        if (args.length == 0) {
            System.out.println("No file is provided!");
            System.exit(-1);
            return;
        }

        File file = new File(args[0]);
        if (file.exists()) {
            System.out.println("File already exists!");
            System.exit(-1);
            return;
        }

        Properties config = new Properties();
        config.put("bootstrap.servers", "localhost:9092");
        config.put("key.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class.getName());
        config.put("value.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class.getName());
        config.put("client.id", "topic-to-file");
        config.put("group.id", "topic-to-file");
        config.put("auto.offset.reset", "earliest");
        config.put("enable.auto.commit", "false");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(config)) {
            consumer.subscribe(Arrays.asList("inventory_purchases"));

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(3));
                    if (records.isEmpty()) {
                        System.out.println("No event remains, going to end");
                        break;
                    }

                    records.forEach(record -> {
                        try {
                            writer.write(String.format("key=%s, value=%s, topic=%s, partition=%d, offset=%d",
                                    record.key(), record.value(), record.topic(), record.partition(), record.offset()));
                        } catch (IOException e) {
                            System.out.println(e.getMessage());
                            System.exit(-1);
                        }
                    });
                }
            } catch (IOException e) {
                System.out.println(e.getMessage());
                System.exit(-1);
            }
        }
    }
}

package com.linuxacademy.ccdak.kafkaSimpleConsumer;

import com.google.gson.Gson;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class PurchaseConsumer {
    public void consume(int recordCount, String outputFile) {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:9092");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "schema-registry-lab");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer.class.getName());
        config.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        KafkaConsumer<String, Purchase> consumer = new KafkaConsumer<>(config);
        consumer.subscribe(Arrays.asList("inventory_purchases"));
        int consumedRecords = 0;
        Gson gson = new Gson();

        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
            while (consumedRecords < recordCount) {
                ConsumerRecords<String, Purchase> records = consumer.poll(Duration.ofSeconds(3));
                consumedRecords += records.count();
                records.forEach(record -> {
                    try {
                        writer.write(gson.toJson(record.value()));
                        writer.write("\n");
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

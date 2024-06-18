package com.linuxacademy.ccdak.kafkaSimpleConsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Main {

    public static void main(String[] args) {

        Properties config=new Properties();

        config.put("bootstrap.servers","localhost:9092");
        config.put("enable.auto.commit","true");
        config.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        config.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        config.put("auto.offset.reset","earliest");

        try(KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(config)){
            consumer.subscribe(Arrays.asList("inventory purchases"));

            while(true){
                ConsumerRecords<String,String> events=consumer.poll(Duration.ofSeconds(1));
                if (events.isEmpty()) {
                    System.out.println("No events any more!");
                    return;
                }
                for(ConsumerRecord<String,String> event:events){
                    System.out.format("%s\n", event.value());
                }
            }
        }
    }

}

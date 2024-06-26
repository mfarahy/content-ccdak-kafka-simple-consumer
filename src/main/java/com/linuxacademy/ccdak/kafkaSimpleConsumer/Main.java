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
        config.put("allow.auto.create.topics","false");
        config.put("group.id","test");

        try(KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(config)){
            consumer.subscribe(Arrays.asList("inventory_purchases"));

            while(true){
                ConsumerRecords<String,String> events=consumer.poll(Duration.ofSeconds(1));
                if (events.isEmpty()) {
                    System.out.println("No events any more!");
                    return;
                }
                for(ConsumerRecord<String,String> event:events){
                    System.out.format("key: %s, value: %s, offset: %d, partition: %d \n",
                            event.key(), event.value(), event.offset(), event.partition());
                }
            }
        }
    }

}

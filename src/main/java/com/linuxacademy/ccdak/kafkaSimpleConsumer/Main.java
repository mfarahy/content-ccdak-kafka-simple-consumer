package com.linuxacademy.ccdak.kafkaSimpleConsumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class Main {

    public static void main(String[] args) {

        if (args.length == 0) {
            System.out.println("No topic has been provided!");
            return;
        }

        var config=new Properties();

        config.put("bootstrap.servers","localhost:9092");
        config.put("enable.auto.commit","true");
        config.put("key.deserializer","org.apache.kafka.common.serializer.StringDeserializer");
        config.put("value.deserializer","org.apache.kafka.common.serializer.StringDeserializer");
        config.put("auto.offset.reset","earliest");

        try(var consumer = new KafkaConsumer<String, String>(config)){
            consumer.subscribe(List.of(args[0]));

            while(true){
                var events=consumer.poll(Duration.ofSeconds(1));
                if (events.isEmpty()) {
                    System.out.println("No events any more!");
                    return;
                }
                for(var event:events){
                    System.out.format("%s\n", event.value());
                }
            }
        }
    }

}

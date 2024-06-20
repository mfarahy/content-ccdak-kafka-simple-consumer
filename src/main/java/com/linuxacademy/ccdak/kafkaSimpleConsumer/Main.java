package com.linuxacademy.ccdak.kafkaSimpleConsumer;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Main {

    public static void main(String[] args) {

        final Properties properties = getProperties();

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> source = builder
                .stream("inventory_purchases");

        final KStream<String, Integer> integerValueSource = source.mapValues(value -> {
            try {
                return Integer.valueOf(value);
            } catch (NumberFormatException e) {
                System.out.format("Unable to convert to integer: %s\n", value);
                return 0;
            }
        });

        final KTable<String, Integer> productCounts = integerValueSource
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
                .reduce((total, newQuantity) -> total + newQuantity);

        productCounts.toStream()
                .to("total_purchases", Produced.with(Serdes.String(), Serdes.Integer()));

       final Topology topology= builder.build();
        try (KafkaStreams streams = new KafkaStreams(topology, properties)) {
            System.out.println(topology.describe());

            final CountDownLatch latch = new CountDownLatch(1);

            Runtime.getRuntime().addShutdownHook(new Thread("shutdown-hook") {
                @Override
                public void run() {
                    streams.close();
                    latch.countDown();
                }
            });

            try {
                streams.start();
                latch.await();
            } catch (InterruptedException e) {
                System.out.println(e.getMessage());
                System.exit(-1);
            }
        }
    }

    private static Properties getProperties() {
        final Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "inventory-data");
        properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        return properties;
    }

}

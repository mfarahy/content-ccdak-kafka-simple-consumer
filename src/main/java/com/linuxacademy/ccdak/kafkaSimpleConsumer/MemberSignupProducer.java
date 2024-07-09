package com.linuxacademy.ccdak.kafkaSimpleConsumer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class MemberSignupProducer implements AutoCloseable {
    private final ProducerOptions _options;
    private Producer<Integer, String> _producer;

    public MemberSignupProducer(ProducerOptions _options) {
        this._options = _options;
        final Properties config = new Properties();

        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, _options.bootstrapServers);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "0");

        _producer = new KafkaProducer<>(config);
    }

    public void set_producer(Producer<Integer, String> producer) {
        this._producer = producer;
    }

    public Producer<Integer, String> getProducer() {
        return _producer;
    }

    public void handleMemberSignup(Integer memberId, String name) {
        int partition;
        if (name.toUpperCase().charAt(0) <= 'M') {
            partition = 0;
        } else {
            partition = 1;
        }
        ProducerRecord<Integer, String> record = new ProducerRecord<>(_options.topic, partition, memberId, name.toLowerCase());

        this._producer.send(record, (RecordMetadata metadata, Exception e) -> {
            if (e != null) {
                System.err.println(e.getMessage());
            } else {
                System.out.println("key=" + record.key() + ", value=" + record.value());
            }
        });
    }

    @Override
    public void close() throws Exception {
        _producer.close();
    }
}

package com.linuxacademy.ccdak.kafkaSimpleConsumer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class MemberSignupProducer {
    private final ProducerOptions _options;
    private Producer<Integer, String> _producer;

    public MemberSignupProducer(ProducerOptions _options) {
        this._options = _options;
        final Properties config = new Properties();

        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
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

        ProducerRecord<Integer, String> record = new ProducerRecord<>(_options.topic(), memberId, name);


    }
}

package com.linuxacademy.ccdak.kafkaSimpleConsumer;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Assert;
import org.junit.Test;

public class MainTest {

    @Test
    public void testAppHasAGreeting() {
        ProducerOptions options = new ProducerOptions();
        options.topic = "test-topic";
        options.bootstrapServers = "localhost:9092";
        MemberSignupProducer producer = new MemberSignupProducer(options);
        try (MockProducer<Integer, String> mockProducer = new MockProducer<>(true,  new IntegerSerializer(),new StringSerializer())) {
            producer.set_producer(mockProducer);
            producer.handleMemberSignup(1, "Max");
            Assert.assertSame(1, mockProducer.history().size());
        }
    }

}

package com.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

public class KafkaProducer {

    private static org.apache.kafka.clients.producer.KafkaProducer<String, byte[]> kafkaProducer;

    public KafkaProducer() {
        Properties props = new Properties();
        try {
            props.load(getClass().getClassLoader().getResourceAsStream("producer.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        kafkaProducer = new org.apache.kafka.clients.producer.KafkaProducer(props);
    }

    public void send(String topic, String key, byte[] value) {
        kafkaProducer.send(new ProducerRecord<>(topic, key, value));
    }

    public void stop() {
        kafkaProducer.close();
    }
}

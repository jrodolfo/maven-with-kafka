package com.jrodolfo.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {

        final String bootstrapServers = "127.0.0.1:9092";

        // 1) create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2) create the producer
        // we want the value and the key to be a String
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // 3) create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "hello world");

        // 4) send data - asynchronous!
        producer.send(record);

        // 5) flush and close producer
        producer.flush();
        producer.close();

    }
}

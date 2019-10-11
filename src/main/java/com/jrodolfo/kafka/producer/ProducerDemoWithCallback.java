package com.jrodolfo.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Date;
import java.util.Properties;

public class ProducerDemoWithCallback {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        final String bootstrapServers = "127.0.0.1:9092";

        // 1) create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2) create the producer
        // we want the value and the key to be a String
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        String message;
        String logMessage;

        for (int i=1; i<=10; i++) {

            message = "canada " + Integer.toString(i);

            // 3) create a producer record
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("first_topic", message);

            logMessage = "Message #" + i + " to be sent: " + message;

            logger.info(logMessage);

            // 4) send data - asynchronous!
            // the method onCompletion() executes every time we get a record being sent, or there is an exception
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time a record is successfully sent or an exception is thrown
                    if (e == null) {
                        // the record was successfully sent
                        Timestamp stamp = new Timestamp(recordMetadata.timestamp());
                        Date date = new Date(stamp.getTime());
                        logger.info("\n\nReceived new metadata.\n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + stamp + "\n" +
                                "Date: " + date);
                    } else {
                        logger.error("Error while producing", e);
                        e.printStackTrace();
                    }
                }
            });
        }


        // 5) flush and close producer
        producer.flush();
        producer.close();

    }
}

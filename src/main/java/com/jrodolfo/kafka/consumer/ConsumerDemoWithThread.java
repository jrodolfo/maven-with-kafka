package com.jrodolfo.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {

    private Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());
        logger.info("Running ConsumerDemoWithThread.main()...");
        new ConsumerDemoWithThread().run();
    }

    private ConsumerDemoWithThread() {
    }

    private void run() {
        logger.info("Running ConsumerDemoWithThread.run()...");
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-sixth-3-application";
        String topic = "first_topic";

        // latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        // create consumer configs
        Properties properties = new Properties();
        String stringName = StringDeserializer.class.getName();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, stringName);
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, stringName);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // create the consumer runnable
        logger.info("Creating the consumer thread");
        ConsumerRunnable myConsumerRunnable = new ConsumerRunnable(
                bootstrapServers,
                groupId,
                topic,
                latch,
                consumer);

        // start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Running Runtime.getRuntime().addShutdownHook()...");
            logger.info("Caught shutdown hook");
            myConsumerRunnable.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
                logger.error("Application got interrupted inside Runtime.getRuntime().addShutdownHook()", e);
            }
            logger.info("Application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing...");
        }
    }

    public static class ConsumerRunnable implements Runnable {

        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;

        ConsumerRunnable(String bootstrapServers,
                         String groupId,
                         String topic,
                         CountDownLatch latch,
                         KafkaConsumer<String, String> consumer) {
            logger.info("Running ConsumerRunnable constructor...");
            this.latch = latch;

            // create consumer
            this.consumer = consumer;
            // subscribe consumer to our topic(s)
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            logger.info("Running ConsumerRunnable.run()...");
            // poll for new data
            ConsumerRecords<String, String> records;
            try {
                while (true) {
                    records = consumer.poll(Duration.ofMillis(100)); // mew is Kafka 2.0.0
                    for (ConsumerRecord record : records) {
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shut down signal!", e);
            } finally {
                if (consumer != null) {
                    consumer.close();
                } else {
                    logger.info("consumer is null");
                }
                // tell our main code we're done with the consumer
                if (latch != null) {
                    latch.countDown();
                } else {
                    logger.info("latch is null");
                }
            }
        }

        void shutdown() {
            // the wakeup() method is a special method to interrupt consumer.poll()
            // it will throw the exception WakeUpException
            logger.info("Running ConsumerRunnable.shutdown()...");
            consumer.wakeup();
        }

    }

}

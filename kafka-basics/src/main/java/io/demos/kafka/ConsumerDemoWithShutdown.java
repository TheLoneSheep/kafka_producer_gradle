package io.demos.kafka;

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

public class ConsumerDemoWithShutdown {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I'm a Kafka consumer!");


        String groupId = "my-java-application";
        String topic = "demo_java";

        // Create Producer Properties
        Properties properties = new Properties();

        // connect to localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // create Consumer configs
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);

        properties.setProperty("auto.offset.reset", "earliest");


        // create a Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);


        // get a reference to the main thread
        final Thread mainThread = Thread.currentThread();


        // add the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();

                // join the main thread to allow execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });





        try {
            // subscribe to a topic
            consumer.subscribe(Arrays.asList(topic));

            // poll data
            while (true) {

                log.info("Polling");

                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record: records) {
                    log.info("key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                }

            }
        } catch (WakeupException e) {
            log.info("consumer is starting to shut down..");
        } catch (Exception e) {
            log.error("unexpected exception in the consumer", e);
        } finally {
            consumer.close();
            log.info("the consumer is now gracefully shut down.");
        }


    }
}

package io.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.EventHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;


import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {
    public static void main(String[] args) throws InterruptedException {
        String bootstrapServers = "127.0.0.1:9092";

        // Create Producer Properties
        Properties properties = new Properties();

        // set Producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());


        // Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        String topic = "wikimedia.recentchange";

        EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource = builder.build();


        // start the producer in another thread
        eventSource.start();

        // we produce for 10 minutes and block until finish
        TimeUnit.MINUTES.sleep(10);
    }
}

package com.github.thiagomatar.poc.consumer;

import com.github.thiagomatar.poc.config.KafkaConsumerPropertiesConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerTest {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerTest.class);
    private static boolean listening = true;

    public static void main(String[] args) {
        final String topic = "demo_topic";
        Properties properties = new KafkaConsumerPropertiesConfig().getDefaultProperty();

        //create  aa consummer
        KafkaConsumer consumer = new KafkaConsumer(properties);

        consumer.subscribe(Collections.singletonList(topic));


        while (listening) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(c -> {
                log.info("Key: {} Value: {}", c.key(), c.value());
                if (c.value().equals("die")) {
                    listening = false;
                }
            });
        }
        consumer.close();

    }

}

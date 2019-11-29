package com.github.thiagomatar.poc.producer;

import com.github.thiagomatar.poc.config.KafkaProducerPropertiesConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducerTest {

    public static void main(String[] args) {
        Properties properties = new KafkaProducerPropertiesConfig().getDefaultProperty();

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        ProducerRecord<String, String> record = new ProducerRecord<String, String>("demo_topic", "Hello World Java!!");
        producer.send(record);
        producer.close();
    }
}

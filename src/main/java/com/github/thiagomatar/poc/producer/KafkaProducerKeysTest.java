package com.github.thiagomatar.poc.producer;

import com.github.thiagomatar.poc.config.KafkaProducerPropertiesConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaProducerKeysTest {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducerKeysTest.class);

    public static void main(String[] args) {

        String message = "Message n";
        for (int i = 0; i < 10; i++) {
            publishToKafka(message + Integer.toString(i), Integer.toString(i));
        }

    }

    private static void publishToKafka(String message, String key) {
        Properties properties = new KafkaProducerPropertiesConfig().getDefaultProperty();

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        // The same key goes to the same partition
        final ProducerRecord<String, String> record = new ProducerRecord<String, String>("test_with_keys", "id_" + key, message);
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    log.info("\n Success: " +
                                    "\n Topic: {}, " +
                                    "\n Partition: {} " +
                                    "\n Offset: {} " +
                                    "\n Timestamp: {}",
                            recordMetadata.topic(),
                            recordMetadata.partition(),
                            recordMetadata.offset(),
                            recordMetadata.timestamp());
                } else {
                    log.error("Error while producing message to kafka, e={}", e.getMessage());
                }
            }
        });
    }
}
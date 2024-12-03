package org.custom.kafkalearning;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducer.class.getSimpleName());
    public static void main(String[] args) {
        // create producer properties.

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "0.0.0.0:9092");

        // Serialization and Deserialization properties.
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create a producer.
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        // create a producer record.
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("price-update", 0,"price1", "{\"price\":56.76, \"availability\":\"N\"}");

        kafkaProducer.send(producerRecord);

        kafkaProducer.flush();
        kafkaProducer.close();

    }
}
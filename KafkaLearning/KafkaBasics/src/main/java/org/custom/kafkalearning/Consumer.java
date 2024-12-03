package org.custom.kafkalearning;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class Consumer {
    public static Logger logger = LoggerFactory.getLogger(Consumer.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("Inside kafka Consumer!");
        String topic = "price-update";
        String group = "my-application";

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "0.0.0.0:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("group.id", group);

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(List.of(topic));
        while (true) {
            logger.info("Polling!");
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
            records.forEach(record -> {
                logger.info("Record Info: \n"
                + " Key: " + record.key() + " Value: " + record.value() + "\n"
                + "Partition: " + record.partition() + " Offset: " + record.offset());
            });
        }
    }
}

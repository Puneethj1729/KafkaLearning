package org.custom.kafkalearning;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallback {
    public static Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class);
    public static void main(String[] args) {
        // create properties.
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "0.0.0.0:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

//        properties.setProperty("batch.size", "10"); Added to reduce batching only in dev - 16k default
//        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName()); - RoundRobin Partitioner. Default is StickyPartitioner.

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        for (int j=0; j<2; j++) {
            for (int i = 0; i < 10; i++) {
                String topic = "price-update";
                String key = "id_" + i;
                String value = "hello world " + i;
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

                kafkaProducer.send(producerRecord, (recordMetadata, e) -> {
                    if (e == null) {
                        logger.info(key + " | Partition: " + recordMetadata.partition());
                    } else {
                        logger.error("Exception: " + e);
                    }
                });
            }
            logger.info("Running for : " + j);

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        kafkaProducer.flush();
        kafkaProducer.close();
    }
}

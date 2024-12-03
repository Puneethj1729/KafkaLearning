package org.custom.kafkalearning;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerWithCooperativeReBalance {
    public static Logger logger = LoggerFactory.getLogger(ConsumerWithCooperativeReBalance.class);
    public static void main(String[] args) {
        String topic = "sample-topic";

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "0.0.0.0:9092");
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());
        properties.put("group.id", "my-kafka-application");
        properties.put("auto.offset.reset", "earliest");
        // This property will only rebalance the selective topic partitions along with different consumers instead of all
        // consumer rebalancing with all consumers
        properties.put("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                logger.info("Detected Shutdown! let's exit by calling consume wakeup..");

                kafkaConsumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        try {
            kafkaConsumer.subscribe(List.of(topic));
            while (true) {
               ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
               logger.info("Polling");
               records.forEach(record -> {
                   logger.info("Record Info: \n"
                           + " Key: " + record.key() + " Value: " + record.value() + "\n"
                           + "Partition: " + record.partition() + " Offset: " + record.offset());
               });
            }
        } catch (WakeupException w) {
            logger.info("Wakeup Exception caught! Can close the consumer gracefully...");
        } catch (Exception e){
            logger.error("Different error: " + e);
        } finally {
            logger.info("Closing the kafka consumer!");
            kafkaConsumer.close();
        }
    }

}

package org.custom.kafkalearning;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerWithShutdown {

    public static Logger logger = LoggerFactory.getLogger(ConsumerWithShutdown.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("Inside kafka Consumer!");
        String topic = "sample-topic";
        String group = "my-application";

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "0.0.0.0:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("group.id", group);

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        // Fetch the mainThread
        final Thread mainThread = Thread.currentThread();

        // Add a Shutdown Hook to trigger when the SIGTERM shutdown occurs.
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                logger.info("Detected Shutdown! let's exit by calling consume wakeup..");
                // This will throw WakeUpException when next consumer.poll method will be invoked.
                kafkaConsumer.wakeup();

                // This part is added to ensure the mainThread shutdown shouldn't happen parallely and must wait until the
                // consumer.poll exceptions are caught. Else, the main thread JVM will also shut down concurrently in parallel
                // to all other threads.
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
                records.forEach(record -> {
                    logger.info("Record Info: \n"
                            + " Key: " + record.key() + " Value: " + record.value() + "\n"
                            + "Partition: " + record.partition() + " Offset: " + record.offset());
                });
            }
        } catch (WakeupException w){
            logger.info("Wakeup exception caught! Consumer can close. "  + w);
        } catch (Exception e) {
            logger.error("Different exception: " + e);
        } finally {
            kafkaConsumer.close();
            logger.info("Kafka Consumer is gracefully shutdown.");
        }
    }
}

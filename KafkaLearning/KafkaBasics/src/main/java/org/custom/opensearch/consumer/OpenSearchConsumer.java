package org.custom.opensearch.consumer;
import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class OpenSearchConsumer {

    public static KafkaConsumer<String, String> createKafkaConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "0.0.0.0:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "opensearch-consumer");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        return kafkaConsumer;
    }
    // Getting unique id from JSON.
    public static String extractId(String json) {
        return JsonParser.parseString(json).getAsJsonObject().get("meta").getAsJsonObject().get("id").getAsString();
    }


    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

        RestHighLevelClient openSearchClient = RestHighLevelClientImpl.createOpenSearchClient();
        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer();

        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(){
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

        try (openSearchClient; kafkaConsumer) {
            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);
            if (!indexExists) {
                openSearchClient.indices().create(new CreateIndexRequest("wikimedia"), RequestOptions.DEFAULT);
                logger.info("Index wikimedia created!");
            } else {
                logger.info("Index wikimedia already exists in Opensearch");
            }
            kafkaConsumer.subscribe(List.of("wikimedia-change"));

            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(3000));
                logger.info("Records Fetched " + records.count());
                BulkRequest bulkRequest = new BulkRequest();
                records.forEach(record -> {
                    try {
//                        String id = record.topic() + "_" + record.partition() + "_" + record.offset(); Using unique ID.
                        String id = extractId(record.value());
                        IndexRequest indexRequest = new IndexRequest("wikimedia").source(record.value(), XContentType.JSON).id(id);
                        bulkRequest.add(indexRequest);
                        // Commented individual index request for performance improvement. We will be doing batch bulk request.
//                        IndexResponse indexResponse = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
//                        logger.info("Inserted One document to the index! " + indexResponse.getId());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
                if (bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    logger.info("Inserted: " + bulkResponse.getItems().length + " records.");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    //Committing offsets manually
                    kafkaConsumer.commitAsync();
                    logger.info("Committing the offsets manually");
                }
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

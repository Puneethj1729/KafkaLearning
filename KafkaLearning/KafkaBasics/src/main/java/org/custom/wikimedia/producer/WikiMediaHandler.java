package org.custom.wikimedia.producer;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikiMediaHandler implements EventHandler {
    KafkaProducer<String, String> producer;
    String topic;
    private final Logger logger = LoggerFactory.getLogger(WikiMediaHandler.class.getSimpleName());
    public WikiMediaHandler(KafkaProducer<String, String> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }
    @Override
    public void onOpen() throws Exception {
        // do nothing
    }

    @Override
    public void onClosed() throws Exception {
        producer.close();
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws Exception {
        logger.info("Sending message : " + messageEvent.getData() + " for event: " + s);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, messageEvent.getData());
        producer.send(producerRecord);
    }

    @Override
    public void onComment(String s) throws Exception {
        // Nothing here
    }

    @Override
    public void onError(Throwable throwable) {
        // Nothing here
    }
}

package io.conductor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChangeHandler implements EventHandler {

    private final KafkaProducer<String, String> kafkaProducer;
    private final String topic;
    private final ObjectMapper objectMapper = new ObjectMapper(); // JSON parser
    private final Logger log = LoggerFactory.getLogger(ChangeHandler.class.getSimpleName());

    public ChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    @Override
    public void onOpen() {
        log.info("Stream opened.");
    }

    @Override
    public void onClosed() {
        log.info("Stream closed.");
        kafkaProducer.close(); // Zatvaranje Kafka producenta
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) {
        try {
            // Parsiranje JSON podataka u Course objekat
            String jsonData = messageEvent.getData();
            Course course = objectMapper.readValue(jsonData, Course.class);

            // Ponovno serijalizovanje objekta u JSON za slanje na Kafka temu
            String serializedData = objectMapper.writeValueAsString(course);

            // Slanje poruke na Kafka temu
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, serializedData);
            kafkaProducer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    log.info("Message sent to topic: {} partition: {} offset: {}",
                            metadata.topic(), metadata.partition(), metadata.offset());
                } else {
                    log.error("Error sending message to Kafka", exception);
                }
            });
        } catch (Exception e) {
            log.error("Error processing message: {}", messageEvent.getData(), e);
        }
    }

    @Override
    public void onComment(String comment) {
        log.info("Comment received: {}", comment);
    }

    @Override
    public void onError(Throwable t) {
        log.error("Error in EventSource stream", t);
    }
}

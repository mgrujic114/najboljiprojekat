package io.conductor;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Producer {

//    private final static Logger log = LoggerFactory.getLogger(org.apache.kafka.clients.producer.Producer.class.getSimpleName());
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String API_URL = "http://localhost:5000/stream/type1";
    private static final String KAFKA_TOPIC = "stream-topic";

    public static void main(String[] args) throws InterruptedException {
        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // safe producer config
        /**
         properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
         properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
         properties.setProperty(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
         */

        // set high throughput producer config
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(32*1024));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");


        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        EventHandler eventHandler = new ChangeHandler(producer, KAFKA_TOPIC);

        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(API_URL));
        EventSource eventSource = builder.build();

        // start the producer in another thread
        eventSource.start();

        // we produce for 10 minutes and block the program until then
        TimeUnit.MINUTES.sleep(1);

//        log.info("Kafka Producer with callback");
//
//        // create Producer properties
//        Properties properties = new Properties();
//        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
//        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//
//        // create the Producer
//        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
//
//        try {
//            int a = 0;
//            // dohvatanje podataka sa API-ja
//            HttpURLConnection connection = (HttpURLConnection) new URL(API_URL).openConnection();
//            connection.setRequestMethod("GET");
//
//            try (BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
//                String line;
//
//                while ((line = reader.readLine()) != null) {
//                    a++;
//                    // kreiranje i slanje poruke
//                    System.out.println(line);
//                    if (a>10) break;
//                    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(KAFKA_TOPIC, line);
//                    producer.send(producerRecord, (recordMetadata, exception) -> {
//                        if (exception == null) {
//                            log.info("Sent message to topic: {} partition: {} offset: {}",
//                                    recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
//                        } else {
//                            log.error("Error sending message", exception);
//                        }
//                    });
//                }
//            }
//        } catch (Exception e) {
//            log.error("Error fetching or sending data", e);
//        } finally {
//            // Sinhronizacija i zatvaranje producenta
//            producer.flush();
//            producer.close();
//            log.info("Kafka Producer closed.");
//        }
//
//        for(int i=0; i<10; i++) {
//            String topic = "topic1";
//            String key = "id_" + i;
//            String value = "hello world " + i;
//
//            // create a producer record
//            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
//
//            // send data - async operation
//            producer.send(producerRecord, new Callback() {
//                // nije samo slanje nego i provera dal smo uspesno poslali
//                @Override
//                public void onCompletion(RecordMetadata metadata, Exception exception) {
//                    // executes every time a record is successfully sent or an exception is thrown
//                    if (exception == null)
//                        log.info("Recieved new metadata" +
//                                "\nTopic: " + metadata.topic() +
//                                "\nKey: " + producerRecord.key() +
//                                "\nPartition: " + metadata.partition() +
//                                "\nOffset: " + metadata.offset() +
//                                "\nTimestamp: " + metadata.timestamp());
//                    else
//                        log.error("Error while producing", exception);
//                }
//            });
//        }
//
        // flush - sync operation
//        producer.flush();
//
//        // close the Producer
//        producer.close();
    }
}


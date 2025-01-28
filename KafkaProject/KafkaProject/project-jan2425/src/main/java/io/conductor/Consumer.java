package io.conductor;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {

    private final static Logger log = LoggerFactory.getLogger(Producer.class.getSimpleName());
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String KAFKA_TOPIC = "stream-topic";

    public static void main(String[] args) {
        log.info("Kafka Consumer");

        String groupId = "jan";

        // create Consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create the Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // get reference to a current thread
        final Thread mainThread = Thread.currentThread();

        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected shutdown, exit by calling consumer.wakeup()...");
                consumer.wakeup();
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            // subscribe Consumer on topic
            consumer.subscribe(Arrays.asList(KAFKA_TOPIC));

            // poll new data
            while (true) {
                log.info("Polling...");

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + ", Value: " + record.value() + "\n" +
                            "Partition: " + record.partition() + ", Offset: " + record.offset() + "\n");
                }
            }
        } catch (WakeupException e) {
            log.info("Wakeup exception");
            // we ignore this because this is an expected exception when closing a consumer
        } catch (Exception exception) {
            log.error("Unexpected exception", exception);
        } finally {
            consumer.close(); // this will also commit the offset if needed
            log.info("The consumer is now successfully closed");
        }
    }
}

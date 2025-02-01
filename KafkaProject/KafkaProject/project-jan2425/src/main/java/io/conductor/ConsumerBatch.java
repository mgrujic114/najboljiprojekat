package io.conductor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ConsumerBatch {

    private final static Logger log = LoggerFactory.getLogger(ConsumerBatch.class.getSimpleName());
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String KAFKA_TOPIC = "batch-topic";
    private static final String CSV_FILE = "courses_fr_2.csv";
    private static final String CLOSE_MSG = "close";

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static Boolean LOOP = true;


    public static void main(String[] args) {
        log.info("Kafka Consumer");

        String groupId = "jan";

        // Create Consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create the Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Get reference to the current thread
        final Thread mainThread = Thread.currentThread();

        // Add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected shutdown, exit by calling consumer.wakeup()...");
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        FileWriter csvWriter = null;
        try {
            csvWriter = new FileWriter(CSV_FILE);
            // Write CSV header
            csvWriter.append("id;title;/url;/is_paid;/instructor_names;/category;/headline;/num_subscribers;/rating;/num_reviews;/instructional_level;/objectives;/curriculum\n");

            // Subscribe to the Kafka topic
            consumer.subscribe(Arrays.asList(KAFKA_TOPIC));

            // Poll for new data
            while (LOOP) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
//                    log.info("Received message: {}", record.value());
                    if (record.value().equalsIgnoreCase(CLOSE_MSG)){
//                        closeConsumer(consumer);
                        LOOP = false;
                        break;
                    }
                        // Parse the JSON batch
                    List<Course> courses = parseBatch(record.value());
                        // Write each course to the CSV file
                    for (Course course : courses) {
                        csvWriter.append(course.getId())
                                .append(";/")
                                .append(course.getTitle())
                                .append(";/")
                                .append(course.getUrl())
                                .append(";/")
                                .append(course.getPaid())
                                .append(";/")
                                .append(course.getInstructorNames())
                                .append(";/")
                                .append(course.getCategory())
                                .append(";/")
                                .append(course.getHeadline())
                                .append(";/")
                                .append(String.valueOf(course.getNumSubscribers()))
                                .append(";/")
                                .append(String.valueOf(course.getRating()))
                                .append(";/")
                                .append(String.valueOf(course.getNumReviews()))
                                .append(";/")
                                .append(course.getInstructionalLevel())
                                .append(";/")
                                .append(course.getObjectives())
                                .append(";/")
                                .append(course.getCurriculum())
                                .append("\n");
                    }
                    // Flush the CSV writer to ensure data is written to the file
                    csvWriter.flush();
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer shutdown initiated...");
        } catch (IOException e) {
            log.error("Error initializing or writing to CSV file", e);
        } finally {
            closeConsumer(consumer);
            try{
                csvWriter.close();
            } catch (IOException e) {
                System.out.println("crkni");
            }
        }
    }

    // Method to parse the JSON batch into a list of Course objects
    private static List<Course> parseBatch(String batchJson) throws IOException {
        JsonNode rootNode = objectMapper.readTree(batchJson);
        return objectMapper.convertValue(rootNode, objectMapper.getTypeFactory().constructCollectionType(List.class, Course.class));
    }

    private static void closeConsumer(KafkaConsumer<String, String>  consumer) {
        consumer.close();
        log.info("Kafka Consumer closed.");
    }

}

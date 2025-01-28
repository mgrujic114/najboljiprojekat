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
    private static final String CSV_FILE = "courses.csv";
    private static final ObjectMapper objectMapper = new ObjectMapper();

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

        try (FileWriter csvWriter = new FileWriter(CSV_FILE)) {
            // Write CSV header
            csvWriter.append("id,title,url,is_paid,instructor_names,category,headline,num_subscribers,rating,num_reviews,instructional_level,objectives,curriculum\n");

            // Subscribe to the Kafka topic
            consumer.subscribe(Arrays.asList(KAFKA_TOPIC));

            // Poll for new data
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Received message: {}", record.value());

                    try {
                        // Parse the JSON batch
                        List<Course> courses = parseBatch(record.value());

                        // Write each course to the CSV file
                        for (Course course : courses) {
                            csvWriter.append(course.getId())
                                    .append(",")
                                    .append(course.getTitle())
                                    .append(",")
                                    .append(course.getUrl())
                                    .append(",")
                                    .append(course.getPaid())
                                    .append(",")
                                    .append(course.getInstructorNames())
                                    .append(",")
                                    .append(course.getCategory())
                                    .append(",")
                                    .append(course.getHeadline())
                                    .append(",")
                                    .append(String.valueOf(course.getNumSubscribers()))
                                    .append(",")
                                    .append(String.valueOf(course.getRating()))
                                    .append(",")
                                    .append(String.valueOf(course.getNumReviews()))
                                    .append(",")
                                    .append(course.getInstructionalLevel())
                                    .append(",")
                                    .append(course.getObjectives())
                                    .append(",")
                                    .append(course.getCurriculum())
                                    .append("\n");
                        }

                        // Flush the CSV writer to ensure data is written to the file
                        csvWriter.flush();

                    } catch (Exception e) {
                        log.error("Error processing record", e);
                    }
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer shutdown initiated...");
        } catch (IOException e) {
            log.error("Error initializing or writing to CSV file", e);
        } finally {
            consumer.close();
            log.info("Kafka Consumer closed.");
        }
    }

    // Method to parse the JSON batch into a list of Course objects
    private static List<Course> parseBatch(String batchJson) throws IOException {
        JsonNode rootNode = objectMapper.readTree(batchJson);
        return objectMapper.convertValue(rootNode, objectMapper.getTypeFactory().constructCollectionType(List.class, Course.class));
    }
}

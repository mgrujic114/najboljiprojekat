package io.conductor;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ProducerBatch {

    private final static Logger log = LoggerFactory.getLogger(ProducerBatch.class.getSimpleName());
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String API_URL = "http://localhost:5000/stream/type1";
    private static final String KAFKA_TOPIC = "batch-topic";
    private static final int BATCH_SIZE = 100;  // Batch size for each message

    public static void main(String[] args) {
        log.info("Kafka Producer");

        // Create Producer properties with batching settings
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");  // Ensure data durability
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "16384");  // Set batch size (16KB)
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "100");  // Wait for 100ms to accumulate a batch

        // Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        try {
            // Fetch data from API and process it
            List<Course> courses = getData();

            // Initialize the list to hold batches of courses
            List<Course> currentBatch = new ArrayList<>();

            for (Course course : courses) {
                currentBatch.add(course);  // Add the current course to the batch

                // Check if the batch size has been reached
                if (currentBatch.size() >= BATCH_SIZE) {
                    // Send the current batch of courses to Kafka
                    sendBatch(producer, currentBatch);
                    // Clear the batch for the next set of courses
                    currentBatch.clear();
                }
            }

            // Send any remaining courses that didn't make a full batch
            if (!currentBatch.isEmpty()) {
                sendBatch(producer, currentBatch);
            }

        } catch (Exception e) {
            log.error("Error fetching or sending data", e);
        } finally {
            // Flush and close the producer
            producer.flush();
            producer.close();
            log.info("Kafka Producer closed.");
        }
    }

    // Method to fetch and process data from the API
    private static List<Course> getData() throws Exception {
        HttpURLConnection connection = (HttpURLConnection) new URL(API_URL).openConnection();
        connection.setRequestMethod("GET");
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(5000);

        if (connection.getResponseCode() != 200) {
            throw new RuntimeException("Failed to connect to API, HTTP code: " + connection.getResponseCode());
        }

        BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        String inputLine;
        List<Course> courses = new ArrayList<>();
        ObjectMapper objectMapper = new ObjectMapper();

        // Read line by line
        while ((inputLine = in.readLine()) != null) {
            // Check if the line starts with "data:" which contains the course object
            if (inputLine.startsWith("data:")) {
                String jsonData = inputLine.substring(5).trim(); // Extract the JSON string after "data:"
                try {
                    // Parse the JSON data into a Course object
                    Course course = objectMapper.readValue(jsonData, Course.class);
                    courses.add(course); // Add to the list
                } catch (Exception e) {
                    log.error("Error parsing course data: " + jsonData, e);
                }
            }
        }
        in.close();
        return courses;
    }

    private static void sendBatch(KafkaProducer<String, String> producer, List<Course> batch) {
        try {
            // Convert the batch of courses to a JSON string
            ObjectMapper objectMapper = new ObjectMapper();
            String batchJson = objectMapper.writeValueAsString(batch);

            // Create a ProducerRecord with the topic and JSON payload
            ProducerRecord<String, String> record = new ProducerRecord<>(KAFKA_TOPIC, batchJson);

            producer.send(record, new Callback() {
                // nije samo slanje nego i provera dal smo uspesno poslali
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // executes every time a record is successfully sent or an exception is thrown
                    if (exception == null)
                        log.info("Recieved new metadata" +
                                "\nTopic: " + metadata.topic() +
                                "\nPartition: " + metadata.partition() +
                                "\nOffset: " + metadata.offset() +
                                "\nTimestamp: " + metadata.timestamp());
                    else
                        log.error("Error while producing", exception);
                }
            });
        } catch (Exception e) {
            log.error("Failed to send batch to Kafka", e);
        }
    }

}

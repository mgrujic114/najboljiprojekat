package io.conductor;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.get.GetIndexRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class Consumer {

    private final static Logger log = LoggerFactory.getLogger(Producer.class.getSimpleName());
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String KAFKA_TOPIC = "stream-topic";
    private static final String INDEX_NAME = "stream-index";


    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";
        URI connUri = URI.create(connString);
        RestHighLevelClient restHighLevelClient;

        restHighLevelClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        return restHighLevelClient;
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        String groupId = "consumer-opensearch-demo";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return new KafkaConsumer<>(properties);
    }

    private static String extractId(String value) {
        return JsonParser.parseString(value).getAsJsonObject().get("meta").getAsJsonObject().get("id").getAsString();
    }

    public static void main(String[] args) throws IOException {
        log.info("Kafka Consumer");
        // first create OpenSerach client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        // create Kafka consumer
        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        //try(openSearchClient; consumer) {
        boolean exist = openSearchClient.indices().exists(new org.opensearch.client.indices.GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

        if (!exist) {
            // create the index on OpenSearch if it doesn't exist already
            org.opensearch.client.indices.CreateIndexRequest createIndexRequest = new org.opensearch.client.indices.CreateIndexRequest("wikimedia");
            openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            log.info("The wikimedia index has been created!");
        } else
            log.info("The wikimedia index already exist!");

        consumer.subscribe(Collections.singleton(KAFKA_TOPIC));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
            log.info("Received {} record(s)", records.count());

            BulkRequest bulkRequest = new BulkRequest();

            for (ConsumerRecord<String, String> record : records) {
                try {
                    String id = extractId(record.value());
                    IndexRequest indexRequest = new IndexRequest(INDEX_NAME)
                            .source(record.value(), XContentType.JSON)
                            .id(id);
                    bulkRequest.add(indexRequest);
                } catch (Exception e) {
                    log.error("Error processing record: {}", record.value(), e);
                }
            }
            if (bulkRequest.numberOfActions() > 0) {
                BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                log.info("Inserted {} records", bulkResponse.getItems().length);

                try{
                    Thread.sleep(1000);
                }catch (InterruptedException ie){
                    ie.printStackTrace();
                }

                consumer.commitSync();
                log.info("Offsets committed!");
            }
        }
    }
//
//        String groupId = "jan";
//
//        // create Consumer properties
//        Properties properties = new Properties();
//        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
//        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
//        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
//        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
//        // create the Consumer
//        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
//
//        // get reference to a current thread
//        final Thread mainThread = Thread.currentThread();
//
//        // adding the shutdown hook
//        Runtime.getRuntime().addShutdownHook(new Thread() {
//            public void run() {
//                log.info("Detected shutdown, exit by calling consumer.wakeup()...");
//                consumer.wakeup();
//                try {
//                    mainThread.join();
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//        });
//
//        try {
//            // subscribe Consumer on topic
//            consumer.subscribe(Arrays.asList(KAFKA_TOPIC));
//
//            // poll new data
//            while (true) {
//                log.info("Polling...");
//
//                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
//
//                for (ConsumerRecord<String, String> record : records) {
//                    log.info("Key: " + record.key() + ", Value: " + record.value() + "\n" +
//                            "Partition: " + record.partition() + ", Offset: " + record.offset() + "\n");
//                }
//            }
//        } catch (WakeupException e) {
//            log.info("Wakeup exception");
//            // we ignore this because this is an expected exception when closing a consumer
//        } catch (Exception exception) {
//            log.error("Unexpected exception", exception);
//        } finally {
//            consumer.close(); // this will also commit the offset if needed
//            log.info("The consumer is now successfully closed");
//        }
//    }
}
